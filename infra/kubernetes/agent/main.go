package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const maxCommandRequestBytes = 1 << 20

const defaultCommandHeartbeat = 10 * time.Second

var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type commandRequest struct {
	Command string            `json:"command"`
	Cwd     string            `json:"cwd"`
	Timeout *float64          `json:"timeout"`
	EnvVars map[string]string `json:"env_vars"`
}

type commandEvent struct {
	Type     string `json:"type"`
	Data     string `json:"data,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
}

func newAgentHandler(token string) http.Handler {
	return newAgentHandlerWithHeartbeat(token, defaultCommandHeartbeat)
}

func newAgentHandlerWithHeartbeat(token string, heartbeat time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/command", func(response http.ResponseWriter, request *http.Request) {
		handleCommand(response, request, heartbeat)
	})
	mux.HandleFunc("PUT /v1/files", handleUpload)
	mux.HandleFunc("GET /v1/files", handleDownload)
	return authenticate(token, mux)
}

func authenticate(token string, next http.Handler) http.Handler {
	expected := []byte("Bearer " + token)
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		provided := []byte(request.Header.Get("Authorization"))
		if len(provided) != len(expected) || subtle.ConstantTimeCompare(provided, expected) != 1 {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(response, request)
	})
}

func decodeCommandRequest(request *http.Request) (commandRequest, error) {
	var payload commandRequest
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxCommandRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return payload, fmt.Errorf("invalid command request: %w", err)
	}
	if payload.Command == "" {
		return payload, errors.New("command must not be empty")
	}
	if payload.Timeout != nil && *payload.Timeout <= 0 {
		return payload, errors.New("timeout must be positive")
	}
	for name := range payload.EnvVars {
		if !environmentName.MatchString(name) {
			return payload, fmt.Errorf("invalid environment variable: %s", name)
		}
	}
	return payload, nil
}

func handleCommand(response http.ResponseWriter, request *http.Request, heartbeat time.Duration) {
	payload, err := decodeCommandRequest(request)
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}

	commandContext, cancelCommand := context.WithCancel(request.Context())
	defer cancelCommand()
	if payload.Timeout != nil {
		timeoutContext, cancelTimeout := context.WithTimeout(
			commandContext,
			time.Duration(*payload.Timeout*float64(time.Second)),
		)
		defer cancelTimeout()
		commandContext = timeoutContext
	}

	command := exec.Command("sh", "-lc", payload.Command)
	command.Dir = payload.Cwd
	command.Env = os.Environ()
	for name, value := range payload.EnvVars {
		command.Env = append(command.Env, name+"="+value)
	}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := command.StdoutPipe()
	if err != nil {
		http.Error(response, "could not open command stdout", http.StatusInternalServerError)
		return
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		http.Error(response, "could not open command stderr", http.StatusInternalServerError)
		return
	}
	if err := command.Start(); err != nil {
		http.Error(response, "could not start command: "+err.Error(), http.StatusBadRequest)
		return
	}

	processDone := make(chan struct{})
	go terminateOnContext(commandContext, command.Process.Pid, processDone)
	events := make(chan commandEvent, 16)
	var readers sync.WaitGroup
	readers.Add(2)
	go readCommandOutput(stdout, "stdout", events, &readers)
	go readCommandOutput(stderr, "stderr", events, &readers)
	waitResult := make(chan error, 1)
	go func() {
		readers.Wait()
		waitResult <- command.Wait()
		close(processDone)
		close(events)
	}()

	response.Header().Set("Content-Type", "application/x-ndjson")
	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("X-Accel-Buffering", "no")
	response.WriteHeader(http.StatusOK)
	flusher, _ := response.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	encoder := json.NewEncoder(response)
	heartbeats := time.NewTicker(heartbeat)
	defer heartbeats.Stop()
	writeFailed := false
	eventsOpen := true
	for eventsOpen {
		select {
		case event, open := <-events:
			if !open {
				eventsOpen = false
				continue
			}
			if writeFailed {
				continue
			}
			if err := encoder.Encode(event); err != nil {
				writeFailed = true
				cancelCommand()
				continue
			}
			if flusher != nil {
				flusher.Flush()
			}
		case <-heartbeats.C:
			if writeFailed {
				continue
			}
			if _, err := io.WriteString(response, "\n"); err != nil {
				writeFailed = true
				cancelCommand()
				continue
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}

	exitCode := commandExitCode(<-waitResult, commandContext.Err())
	if writeFailed {
		return
	}
	if err := encoder.Encode(commandEvent{Type: "exit", ExitCode: &exitCode}); err == nil && flusher != nil {
		flusher.Flush()
	}
}

func readCommandOutput(reader io.Reader, eventType string, events chan<- commandEvent, readers *sync.WaitGroup) {
	defer readers.Done()
	buffer := make([]byte, 32*1024)
	var carry []byte
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			data := make([]byte, 0, len(carry)+count)
			data = append(data, carry...)
			data = append(data, buffer[:count]...)
			text, remainder := decodeUTF8(data, err != nil)
			carry = remainder
			if text != "" {
				events <- commandEvent{Type: eventType, Data: text}
			}
		}
		if err != nil {
			if len(carry) > 0 {
				text, _ := decodeUTF8(carry, true)
				if text != "" {
					events <- commandEvent{Type: eventType, Data: text}
				}
			}
			return
		}
	}
}

func decodeUTF8(data []byte, atEOF bool) (string, []byte) {
	decoded := make([]byte, 0, len(data))
	for len(data) > 0 {
		runeValue, size := utf8.DecodeRune(data)
		if runeValue == utf8.RuneError && size == 1 {
			if !atEOF && !utf8.FullRune(data) {
				return string(decoded), append([]byte(nil), data...)
			}
			decoded = utf8.AppendRune(decoded, utf8.RuneError)
			data = data[1:]
			continue
		}
		decoded = append(decoded, data[:size]...)
		data = data[size:]
	}
	return string(decoded), nil
}

func terminateOnContext(ctx context.Context, processID int, processDone <-chan struct{}) {
	select {
	case <-ctx.Done():
	case <-processDone:
		return
	}
	_ = syscall.Kill(-processID, syscall.SIGTERM)
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-processDone:
		return
	case <-timer.C:
		_ = syscall.Kill(-processID, syscall.SIGKILL)
	}
}

func commandExitCode(waitError error, contextError error) int {
	if errors.Is(contextError, context.DeadlineExceeded) {
		return 124
	}
	if errors.Is(contextError, context.Canceled) {
		return 143
	}
	if waitError == nil {
		return 0
	}
	var exitError *exec.ExitError
	if errors.As(waitError, &exitError) {
		return exitError.ExitCode()
	}
	return 1
}

func filePath(request *http.Request) (string, error) {
	path := request.URL.Query().Get("path")
	if path == "" {
		return "", errors.New("path is required")
	}
	return path, nil
}

func handleUpload(response http.ResponseWriter, request *http.Request) {
	path, err := filePath(request)
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		http.Error(response, "could not create upload directory: "+err.Error(), http.StatusBadRequest)
		return
	}
	temporary, err := os.CreateTemp(parent, ".vals-upload-*")
	if err != nil {
		http.Error(response, "could not create upload: "+err.Error(), http.StatusBadRequest)
		return
	}
	temporaryPath := temporary.Name()
	succeeded := false
	defer func() {
		_ = temporary.Close()
		if !succeeded {
			_ = os.Remove(temporaryPath)
		}
	}()
	if _, err := io.Copy(temporary, request.Body); err != nil {
		http.Error(response, "could not write upload: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := temporary.Chmod(0o644); err != nil {
		http.Error(response, "could not set upload mode: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := temporary.Close(); err != nil {
		http.Error(response, "could not close upload: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		http.Error(response, "could not finish upload: "+err.Error(), http.StatusBadRequest)
		return
	}
	succeeded = true
	response.WriteHeader(http.StatusNoContent)
}

func handleDownload(response http.ResponseWriter, request *http.Request) {
	path, err := filePath(request)
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(response, "file not found", http.StatusNotFound)
			return
		}
		http.Error(response, "could not open file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		http.Error(response, "path is not a regular file", http.StatusBadRequest)
		return
	}
	response.Header().Set("Content-Type", "application/octet-stream")
	response.Header().Set("Cache-Control", "no-store")
	http.ServeContent(response, request, filepath.Base(path), info.ModTime(), file)
}

func main() {
	token := os.Getenv("VALS_SANDBOX_AGENT_TOKEN")
	if token == "" {
		log.Fatal("VALS_SANDBOX_AGENT_TOKEN is required")
	}
	port := 8787
	if configured := os.Getenv("VALS_SANDBOX_AGENT_PORT"); configured != "" {
		parsed, err := strconv.Atoi(configured)
		if err != nil || parsed < 1 || parsed > 65535 {
			log.Fatal("VALS_SANDBOX_AGENT_PORT must be a valid port")
		}
		port = parsed
	}
	heartbeat := defaultCommandHeartbeat
	if configured := os.Getenv("VALS_SANDBOX_AGENT_HEARTBEAT_SECONDS"); configured != "" {
		seconds, err := strconv.ParseFloat(configured, 64)
		if err != nil || seconds <= 0 {
			log.Fatal("VALS_SANDBOX_AGENT_HEARTBEAT_SECONDS must be positive")
		}
		heartbeat = time.Duration(seconds * float64(time.Second))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           newAgentHandlerWithHeartbeat(token, heartbeat),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}
	go func() {
		<-ctx.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownContext)
	}()
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

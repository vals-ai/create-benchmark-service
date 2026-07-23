package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAgentCommandsAndFiles(t *testing.T) {
	t.Parallel()
	token := "sandbox-token"
	handler := newAgentHandler(token)

	commandBody, err := json.Marshal(commandRequest{Command: "printf first; printf warning >&2; exit 7"})
	if err != nil {
		t.Fatal(err)
	}
	commandRequest := httptest.NewRequest(http.MethodPost, "/v1/command", bytes.NewReader(commandBody))
	commandRequest.Header.Set("Authorization", "Bearer "+token)
	commandResponse := httptest.NewRecorder()
	handler.ServeHTTP(commandResponse, commandRequest)
	if commandResponse.Code != http.StatusOK {
		t.Fatalf("command status = %d, body = %s", commandResponse.Code, commandResponse.Body.String())
	}
	for _, expected := range []string{`"type":"stdout"`, `"data":"first"`, `"type":"stderr"`, `"exit_code":7`} {
		if !strings.Contains(commandResponse.Body.String(), expected) {
			t.Errorf("command response missing %s: %s", expected, commandResponse.Body.String())
		}
	}

	directory := t.TempDir()
	path := filepath.Join(directory, "nested", "result.bin")
	uploadRequest := httptest.NewRequest(http.MethodPut, "/v1/files?path="+path, bytes.NewReader([]byte{0, 1, 2, 255}))
	uploadRequest.Header.Set("Authorization", "Bearer "+token)
	uploadResponse := httptest.NewRecorder()
	handler.ServeHTTP(uploadResponse, uploadRequest)
	if uploadResponse.Code != http.StatusNoContent {
		t.Fatalf("upload status = %d, body = %s", uploadResponse.Code, uploadResponse.Body.String())
	}

	downloadRequest := httptest.NewRequest(http.MethodGet, "/v1/files?path="+path, nil)
	downloadRequest.Header.Set("Authorization", "Bearer "+token)
	downloadResponse := httptest.NewRecorder()
	handler.ServeHTTP(downloadResponse, downloadRequest)
	if !bytes.Equal(downloadResponse.Body.Bytes(), []byte{0, 1, 2, 255}) {
		t.Fatalf("download body = %v", downloadResponse.Body.Bytes())
	}

	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, httptest.NewRequest(http.MethodGet, "/v1/files?path="+path, nil))
	if denied.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", denied.Code)
	}
}

func TestAgentCancelsCommandProcessGroup(t *testing.T) {
	t.Parallel()
	marker := filepath.Join(t.TempDir(), "finished")
	body, err := json.Marshal(commandRequest{Command: "sleep 5; touch " + marker})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/v1/command", bytes.NewReader(body)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer token")
	done := make(chan struct{})
	go func() {
		newAgentHandler("token").ServeHTTP(httptest.NewRecorder(), request)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("command did not stop after request cancellation")
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("cancelled command created marker: %v", err)
	}
}

func TestAgentHeartbeatsIdleCommands(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(commandRequest{Command: "sleep 0.05"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/command", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()

	newAgentHandlerWithHeartbeat("token", 5*time.Millisecond).ServeHTTP(response, request)

	if !strings.HasPrefix(response.Body.String(), "\n") {
		t.Fatalf("idle command did not start with a heartbeat: %q", response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"type":"exit"`) {
		t.Fatalf("idle command did not finish: %q", response.Body.String())
	}
}

func TestAgentPreservesFinalOutputUnderConcurrency(t *testing.T) {
	t.Parallel()
	const commands = 250
	token := "sandbox-token"
	handler := newAgentHandler(token)
	body, err := json.Marshal(commandRequest{Command: "printf 'stream-finished\\n'"})
	if err != nil {
		t.Fatal(err)
	}

	var requests sync.WaitGroup
	requests.Add(commands)
	for index := range commands {
		go func() {
			defer requests.Done()
			request := httptest.NewRequest(http.MethodPost, "/v1/command", bytes.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if !strings.Contains(response.Body.String(), `"data":"stream-finished\n"`) {
				t.Errorf("command %d lost final output: %s", index, response.Body.String())
			}
		}()
	}
	requests.Wait()
}

func TestDecodeUTF8Boundaries(t *testing.T) {
	t.Parallel()
	text, carry := decodeUTF8([]byte{'s', 't', 'a', 'r', 't', ' ', 0xe2}, false)
	if text != "start " || !bytes.Equal(carry, []byte{0xe2}) {
		t.Fatalf("first chunk = %q, %v", text, carry)
	}
	text, carry = decodeUTF8(append(carry, 0x82, 0xac), false)
	if text != "€" || len(carry) != 0 {
		t.Fatalf("second chunk = %q, %v", text, carry)
	}
	text, carry = decodeUTF8([]byte{0xff, 0xe2}, true)
	if text != "��" || len(carry) != 0 {
		t.Fatalf("final chunk = %q, %v", text, carry)
	}
}

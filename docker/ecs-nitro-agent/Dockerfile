# Agent image for the ecs_nitro sandbox provider. Build from the repository root:
#   docker build --platform linux/amd64 -f docker/ecs-nitro-agent/Dockerfile -t <ecr-repo>:<tag> .
# Its default command copies a musl CPython, the musl loader and agent.py into the task volume at /cbs-agent.
FROM alpine:3.20 AS fetch
ARG PBS_RELEASE=20261110
ARG PBS_PYTHON=3.12.16
ARG PBS_SHA256=e32685cf248d2f33b249816120774143b4b57578cb30de5b8dba3e54963765c3
ADD --checksum=sha256:${PBS_SHA256} \
    https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_RELEASE}/cpython-${PBS_PYTHON}%2B${PBS_RELEASE}-x86_64-unknown-linux-musl-install_only_stripped.tar.gz \
    /tmp/python.tar.gz
RUN mkdir -p /opt/cbs-agent/lib && tar -xzf /tmp/python.tar.gz -C /opt/cbs-agent \
    && cp /lib/ld-musl-x86_64.so.1 /opt/cbs-agent/lib/

FROM alpine:3.20
COPY --from=fetch /opt/cbs-agent /opt/cbs-agent
COPY src/benchmark_service/sandbox/ecs_nitro/agent.py /opt/cbs-agent/agent.py
RUN /opt/cbs-agent/lib/ld-musl-x86_64.so.1 /opt/cbs-agent/python/bin/python3.12 -I -m py_compile /opt/cbs-agent/agent.py
ENTRYPOINT ["/bin/cp", "-a", "/opt/cbs-agent/.", "/cbs-agent/"]

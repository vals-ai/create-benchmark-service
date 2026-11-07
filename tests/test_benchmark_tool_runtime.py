import json
import os
import runpy
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "src/benchmark_service/tool_runtime/runtime.py"


@pytest.mark.parametrize(
    "tools_uid,tools_gid,failed_startup",
    [(10002, 10002, False), (10002, 0, False), (10002, 10002, True)],
    ids=["split", "docker_gid0", "dead_broker"],
)
def test_split_uid_runtime_in_real_container(tools_uid: int, tools_gid: int, failed_startup: bool) -> None:
    if os.environ.get("BENCHMARK_TOOL_RUNTIME_DOCKER_TESTS") != "1":
        pytest.skip("Set BENCHMARK_TOOL_RUNTIME_DOCKER_TESTS=1 for the root-container check")
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--mount",
            f"type=bind,src={ROOT},dst=/source,readonly",
            "python:3.12-slim",
            "python3",
            "-c",
            (FAILED_STARTUP_CHECK if failed_startup else CONTAINER_CHECK)
            .replace("'tools_gid':10002", f"'tools_gid':{tools_gid}")
            .replace("broker['gid']==10002", f"broker['gid']=={tools_gid}"),
        ],
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads(result.stdout.splitlines()[-1])
    assert report == {"agent_uid": 10001, "tools_uid": tools_uid, "stopped": True}


CONTAINER_CHECK = r'''
import json, os, signal, subprocess, sys, time
from pathlib import Path

runtime = Path('/source/src/benchmark_service/tool_runtime/runtime.py')
root = Path('/run/benchmark-tools')
root.mkdir(parents=True)
workspace = Path('/workspace')
workspace.mkdir()
protected = Path('/app')
protected.mkdir()
(protected/'hidden.txt').write_text('private task data')
(root/'native_tools.py').write_text("""
import json, os, sys, time, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
config=json.loads(Path(sys.argv[1]).read_text())
os.environ.update(config['credentials'])
output=Path(config['output_dir'])
(output/'instructions.txt').write_text('Native task instructions.')
(output/'prompt.txt').write_text('Native task question.')
report={'uid':os.getuid(),'gid':os.getgid(),'groups':os.getgroups(),
        'secret':os.environ.get('TOOL_SECRET'),'proxy':os.environ.get('HTTPS_PROXY'),
        'status':Path('/proc/self/status').read_text()}
(output/'broker.json').write_text(json.dumps(report))
(output/'tool_calls.jsonl').write_text(json.dumps({'result':'[REDACTED]'})+'\\n')
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200); self.end_headers()
        self.wfile.write(b'{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}')
    def log_message(self,*args): pass
server=HTTPServer(('127.0.0.1',config['port']),Handler)
threading.Thread(target=server.serve_forever,daemon=True).start()
while not Path('/workspace/shared.txt').exists(): time.sleep(.01)
with Path('/workspace/shared.txt').open('a') as stream: stream.write(' tools')
(output/'generation-started.txt').write_text(Path('/run/benchmark-tools/generation_started_at').read_text())
with (output/'tool_calls.jsonl').open('a') as stream:
    stream.write(json.dumps({'result':'[REDACTED]'})+'\\n')
(output/'shared-ready').touch()
while True: time.sleep(1)
""")
real = Path('/usr/local/lib/codex-real')
real.write_text("""#!/usr/local/bin/python3
import json, os, sys
from pathlib import Path
if '--version' in sys.argv: print('test Codex'); sys.exit(0)
state=json.loads(Path('/workspace/public-state.json').read_text())
blocked=[]
for name in ['/app/hidden.txt','/run/benchmark-tools/runtime.json',
             '/run/benchmark-tools/native.json','/logs/benchmark_tools/tool_calls.jsonl',f"/proc/{state['broker_pid']}/environ"]:
    try: Path(name).read_bytes()
    except PermissionError: blocked.append(name)
    else: raise AssertionError('Agent read '+name)
for name in ['/usr/local/bin/codex','/run/benchmark-tools/native_tools.py']:
    try: Path(name).write_text('replace trusted code')
    except PermissionError: pass
    else: raise AssertionError('Agent changed '+name)
try: os.setuid(0)
except PermissionError: pass
else: raise AssertionError('Agent regained root')
Path('/workspace/shared.tmp').write_text('agent')
Path('/workspace/shared.tmp').rename('/workspace/shared.txt')
assert Path.cwd()==Path('/workspace')
status=Path('/proc/self/status').read_text()
assert 'NoNewPrivs:\\t1' in status
assert 'CapEff:\\t0000000000000000' in status
assert 'CapBnd:\\t0000000000000000' in status
assert os.getgroups()==[]
assert os.environ['HTTP_PROXY']=='http://agent-proxy.invalid:3128'
assert os.environ['MODEL_GATEWAY_API_KEY']=='mgwt_main-task'
assert 'TOOL_SECRET' not in os.environ
assert 'PYTHONPATH' not in os.environ
assert Path.home()==Path('/run/benchmark-agent/home')
assert Path('/run/benchmark-agent/prompt.txt').read_text()=='Native task question.'
assert '--model' in __import__('sys').argv
assert 'features.shell_tool=false' in __import__('sys').argv
print(json.dumps({'uid':os.getuid(),'blocked':blocked}))
""")
real.chmod(0o755)
Path('/usr/local/bin/codex').symlink_to(real)
config={
 'root':str(root),'workspace':str(workspace),'protected_dirs':[str(protected)],
 'agent_uid':10001,'agent_gid':10001,'tools_uid':10002,'tools_gid':10002,
 'python_path':sys.executable,
 'native':{'benchmark':'fake','model':'test','task_id':'task','question':'Question',
           'as_of_date':'2026-01-01','workbook_path':'/workspace/submission.xlsx',
           'output_dir':'/app/results/task','credentials':{},'port':8765},
 'credential_names':['TOOL_SECRET'],
 'tool_environment':{'HTTPS_PROXY':'http://tools-only-secret@proxy.invalid:3128',
                     'MODEL_GATEWAY_API_KEY':'mgwt_tools-task'},
 'codex_config':{'features.shell_tool':False},
}
config_path=root/'runtime.json'
config_path.write_text(json.dumps(config))
environment={**os.environ,'TOOL_SECRET':'tools-only-secret'}
start=subprocess.run([sys.executable,'-I',str(runtime),'start',str(config_path)],
                     env=environment,capture_output=True,text=True,timeout=30)
assert start.returncode==0,start.stdout+start.stderr
state=json.loads(start.stdout.splitlines()[-1])
(workspace/'public-state.json').write_text(json.dumps(state))
try:
    broker=json.loads(Path('/app/results/task/broker.json').read_text())
    assert broker['uid']==10002 and broker['gid']==10002 and broker['groups']==[]
    assert broker['secret']=='tools-only-secret'
    assert broker['proxy']=='http://tools-only-secret@proxy.invalid:3128'
    assert 'NoNewPrivs:\t1' in broker['status']
    assert 'CapEff:\t0000000000000000' in broker['status']
    marker=root/'generation_started_at'
    version=subprocess.run(['/usr/local/bin/codex','--version'],capture_output=True,text=True,timeout=10)
    assert version.returncode==0,version.stdout+version.stderr
    assert not marker.exists()
    probe=subprocess.run(['/usr/local/bin/codex','exec','--model','test','--'],
        env={**os.environ,'HTTP_PROXY':'http://agent-proxy.invalid:3128',
             'MODEL_GATEWAY_API_KEY':'mgwt_main-task','TOOL_SECRET':'do-not-pass',
             'PYTHONPATH':'/workspace/malicious'},cwd='/app',capture_output=True,text=True,timeout=10)
    assert probe.returncode==0,probe.stdout+probe.stderr
    assert json.loads(probe.stdout)['uid']==10001
    assert 0 < float(marker.read_text()) <= time.monotonic()
    assert marker.stat().st_uid==0 and marker.stat().st_gid==config['tools_gid']
    assert marker.stat().st_mode & 0o777 == 0o440
    deadline=time.monotonic()+5
    while not Path('/app/results/task/shared-ready').exists():
        assert time.monotonic()<deadline
        time.sleep(.01)
    assert (workspace/'shared.txt').read_text()=='agent tools'
    assert Path('/app/results/task/generation-started.txt').read_text()==marker.read_text()
    artifact=Path('/logs/benchmark_tools/tool_calls.jsonl')
    assert artifact.read_text().splitlines()==[json.dumps({'result':'[REDACTED]'})]*2
    assert Path('/app/results/task/tool_calls.jsonl').is_symlink()
    assert Path('/app/results/task/tool_calls.jsonl').resolve()==artifact
    assert artifact.parent.stat().st_uid==0
    assert artifact.parent.stat().st_gid==config['tools_gid']
    assert artifact.parent.stat().st_mode & 0o777 == 0o710
    assert artifact.stat().st_uid==config['tools_uid']
    assert artifact.stat().st_mode & 0o777 == 0o600
    stop=subprocess.run([sys.executable,'-I',str(runtime),'stop',str(config_path)],
                        capture_output=True,text=True,timeout=10)
    assert stop.returncode==0,stop.stdout+stop.stderr
    assert artifact.read_text().splitlines()==[json.dumps({'result':'[REDACTED]'})]*2
    assert not Path('/logs/benchmark_tools/runtime.json').exists()
    proc=Path(f"/proc/{state['broker_pid']}/status")
    assert not proc.exists() or '\nState:\tZ' in proc.read_text()
finally:
    subprocess.run([sys.executable,'-I',str(runtime),'stop',str(config_path)],capture_output=True)
print(json.dumps({'agent_uid':10001,'tools_uid':10002,'stopped':True}))
'''


FAILED_STARTUP_CHECK = CONTAINER_CHECK.replace(
    "report={'uid':os.getuid()", "raise RuntimeError('Graph startup failed')\nreport={'uid':os.getuid()"
).replace(
    "assert start.returncode==0,start.stdout+start.stderr",
    "assert start.returncode != 0, 'Runtime accepted a dead native broker'\n"
    "assert Path('/usr/local/bin/codex').resolve()==real\n"
    "print(json.dumps({'agent_uid':10001,'tools_uid':10002,'stopped':True}))\n"
    "sys.exit(0)",
)


@pytest.mark.parametrize("action", ["start", "stop"])
@pytest.mark.parametrize("agent_uid,tools_uid", [(10001, 0), (0, 10002), (10001, 10001)])
def test_invalid_uids_cannot_reach_filesystem(
    monkeypatch: pytest.MonkeyPatch, action: str, agent_uid: int, tools_uid: int
) -> None:
    runtime = runpy.run_path(str(RUNTIME))
    function = runtime[action]
    monkeypatch.setattr(os, "geteuid", lambda: 0)

    def reject_path(*args: object) -> None:
        pytest.fail("Invalid UIDs reached the filesystem")

    monkeypatch.setitem(function.__globals__, "Path", reject_path)
    config = SimpleNamespace(
        agent_uid=agent_uid,
        tools_uid=tools_uid,
        agent_gid=10001,
        tools_gid=0,
        root="/unused",
        credential_names=[],
        tool_environment={},
    )
    with pytest.raises(AssertionError, match="UID"):
        if action == "start":
            function(config, Path("/unused"))
        else:
            function(config)

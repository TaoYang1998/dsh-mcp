"""Deterministic ACP v1 peer; never invokes a model or launches execution children."""
import json
import os
import signal
import sys
import time

mode = os.environ.get("DSH_MCP_FIXTURE_HANG", "")
if mode:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    with open(os.environ["DSH_MCP_FIXTURE_PID_FILE"], "w") as pid_file:
        pid_file.write(str(os.getpid()))


def send(value):
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", **value}) + "\n")
    sys.stdout.flush()


if os.environ.get("DSH_MCP_FIXTURE_STARTS"):
    with open(os.environ["DSH_MCP_FIXTURE_STARTS"], "a") as counter:
        counter.write(str(os.getpid()) + "\n")
history = os.environ.get("DSH_MCP_FIXTURE_HISTORY") == "1"
cwd = "/tmp"
pending = {}
sessions = 0
for line in sys.stdin:
    request = json.loads(line)
    method, params = request.get("method"), request.get("params", {})
    request_id = request.get("id")
    if method == "initialize":
        if mode == "startup":
            time.sleep(1000)
        result = {"protocolVersion": 1, "agentCapabilities": {"sessionCapabilities": {"close": {}}},
                  "agentInfo": {"name": "deterministic-test-peer", "version": "1"}}
        if history:
            result["agentCapabilities"]["sessionCapabilities"].update({"list": {}, "resume": {}})
    elif method == "session/list":
        result = {"sessions": [{"sessionId": "history:alpha/1", "cwd": cwd}]}
    elif method == "session/resume":
        cwd = params["cwd"]
        result = {}
    elif method == "session/new":
        cwd = params["cwd"]
        sessions += 1
        result = {"sessionId": f"test-session-{sessions}"}
    elif method == "session/prompt":
        pending[params["sessionId"]] = request_id
        if os.environ.get("DSH_MCP_FIXTURE_CRASH") == "1":
            os._exit(23)
        send({"method": "session/update", "params": {"sessionId": params["sessionId"],
              "update": {"sessionUpdate": "agent_message_chunk", "messageId": "m1",
                         "content": {"type": "text", "text": "fixture running"}}}})
        if mode == "disconnected":
            os.close(1)
            time.sleep(1000)
        continue
    elif method == "session/cancel":
        if mode:
            continue
        task = pending.pop(params["sessionId"], None)
        if task is not None:
            send({"id": task, "result": {"stopReason": "cancelled"}})
        continue
    elif method == "session/close":
        if mode:
            continue
        task = pending.pop(params["sessionId"], None)
        if task is not None:
            send({"id": task, "result": {"stopReason": "cancelled"}})
        result = {}
    else:
        send({"id": request_id, "error": {"code": -32601, "message": "unknown test method"}})
        continue
    if request_id is not None:
        send({"id": request_id, "result": result})

if mode:
    time.sleep(1000)

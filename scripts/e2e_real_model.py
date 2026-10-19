#!/usr/bin/env python3
"""Explicit real-model MCP acceptance run; only the Python standard library is used."""

import argparse
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import sys
import tempfile
import time
import uuid


TOOLS = {
    "start_task", "get_task", "send_followup", "cancel_task",
    "resolve_permission", "close_session",
    "list_sessions", "resume_task", "list_tasks", "attach_session", "get_status",
}
TERMINAL = {"completed", "cancelled", "failed"}


class AcceptanceError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise AcceptanceError(message)


class McpClient:
    def __init__(self, bridge, state_dir, request_timeout):
        env = os.environ.copy()
        env["DSH_MCP_STATE_DIR"] = str(state_dir)
        dsh_home = state_dir.parent / "dsh-home"
        dsh_home.mkdir(mode=0o700)
        env["DSH_HOME"] = str(dsh_home)
        self.process = subprocess.Popen(
            [str(bridge)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=None, env=env,
        )
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)
        self.buffer = bytearray()
        self.sequence = 0
        self.request_timeout = request_timeout

    def send(self, message):
        try:
            self.process.stdin.write(json.dumps(message, ensure_ascii=False).encode() + b"\n")
            self.process.stdin.flush()
        except (BrokenPipeError, OSError) as error:
            raise AcceptanceError("MCP stdin closed") from error

    def receive(self, deadline):
        while True:
            newline = self.buffer.find(b"\n")
            if newline >= 0:
                raw = bytes(self.buffer[:newline])
                del self.buffer[:newline + 1]
                try:
                    return json.loads(raw)
                except (UnicodeError, json.JSONDecodeError) as error:
                    raise AcceptanceError("MCP stdout is not a JSON frame") from error
            require(len(self.buffer) <= 1024 * 1024, "MCP frame exceeds 1 MiB")
            remaining = deadline - time.monotonic()
            require(remaining > 0, "MCP request timed out")
            require(self.selector.select(remaining), "MCP request timed out")
            chunk = os.read(self.process.stdout.fileno(), 65536)
            require(chunk, "MCP stdout closed before response")
            self.buffer.extend(chunk)

    def request(self, method, params):
        self.sequence += 1
        request_id = self.sequence
        self.send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        deadline = time.monotonic() + self.request_timeout
        while True:
            response = self.receive(deadline)
            if "method" in response:
                if "id" in response:
                    # The client advertises no roots, sampling, or elicitation capabilities.
                    result = {"result": {}} if response["method"] == "ping" else {
                        "error": {"code": -32601, "message": "Method not supported"}
                    }
                    self.send({"jsonrpc": "2.0", "id": response["id"], **result})
                continue
            require(response.get("id") == request_id, "unexpected MCP response id")
            require("error" not in response, "JSON-RPC request failed")
            return response["result"]

    def initialize(self):
        result = self.request("initialize", {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "dsh-mcp-real-model-acceptance", "version": "1"},
        })
        require(result.get("protocolVersion"), "initialize omitted protocol version")
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        tools = self.request("tools/list", {})
        require({item["name"] for item in tools["tools"]} == TOOLS, "expected eleven MCP tools")

    def tool(self, name, arguments, expected_error=None):
        result = self.request("tools/call", {"name": name, "arguments": arguments})
        body = result.get("structuredContent")
        if body is None:
            for block in result.get("content", []):
                if block.get("type") == "text":
                    try:
                        body = json.loads(block["text"])
                        break
                    except json.JSONDecodeError:
                        continue
        require(isinstance(body, dict), f"{name}: missing structured tool response")
        if expected_error is not None:
            require(result.get("isError") is True, f"{name}: expected business error")
            require(body.get("code") == expected_error, f"{name}: unexpected business error code")
        elif result.get("isError"):
            raise AcceptanceError(f"{name}: {body.get('code', 'unknown_business_error')}")
        return body

    def finish(self, timeout):
        """Close MCP input and wait; forced termination always makes cleanup unconfirmed."""
        forced = False
        try:
            self.process.stdin.close()
        except (BrokenPipeError, OSError):
            pass
        try:
            code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            forced = True
            self.process.terminate()
            try:
                code = self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                code = self.process.wait(timeout=10)
        self.selector.close()
        self.process.stdout.close()
        return code == 0 and not forced


class Acceptance:
    def __init__(self, client, root, args, report):
        self.client, self.root, self.args, self.report = client, root, args, report
        self.tasks = []
        self.decisions = set()

    def start(self, directory, prompt, request_id=None):
        arguments = {"request_id": request_id or str(uuid.uuid4()), "cwd": str(directory), "prompt": prompt}
        accepted = self.client.tool("start_task", arguments)
        self.tasks.append(accepted["task_id"])
        return accepted, arguments

    def snapshot(self, task_id):
        view = self.client.tool("get_task", {"task_id": task_id})
        for permission in view.get("pending_permissions", []):
            permission_id = permission["permission_id"]
            if permission_id in self.decisions:
                continue
            self.report["permission_requests_observed"] += 1
            kind = "allow_once" if self.args.permission_policy == "allow-once" else "reject_once"
            option = next((item for item in permission["options"] if item.get("kind") == kind), None)
            if option is None:
                self.client.tool("cancel_task", {"task_id": task_id})
                raise AcceptanceError("permission lacks the configured one-time decision; task cancelled")
            self.client.tool("resolve_permission", {
                "task_id": task_id, "permission_id": permission_id, "option_id": option["option_id"],
            })
            self.decisions.add(permission_id)
            self.report["permission_requests_resolved"] += 1
        return view

    def wait_terminal(self, task_id, timeout=None):
        deadline = time.monotonic() + (timeout or self.args.task_timeout)
        while time.monotonic() < deadline:
            view = self.snapshot(task_id)
            if view["status"] in TERMINAL:
                require(view["execution_state"] != "unknown", "task execution state is unknown")
                return view
            time.sleep(0.2)
        raise AcceptanceError("task did not reach a terminal state within the acceptance budget")

    def expect_completed(self, task_id):
        view = self.wait_terminal(task_id)
        if view["status"] != "completed":
            code = (view.get("error") or {}).get("code", "no_error_code")
            raise AcceptanceError(f"real-model task ended as {view['status']}: {code}")
        require(view["execution_state"] == "stopped", "completed task has no stop evidence")
        return view

    def close(self, task_id, timeout=30):
        self.client.tool("close_session", {"task_id": task_id})
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            view = self.client.tool("get_task", {"task_id": task_id})
            if view["session"]["state"] == "closed":
                require(not view["session"]["followup_available"], "closed session still permits followup")
                return
            require(view["session"]["state"] != "lost", "session cleanup is unconfirmed")
            time.sleep(0.2)
        raise AcceptanceError("session close did not settle")

    def exercise(self):
        first_dir, second_dir = self.root / "one", self.root / "two"
        first_dir.mkdir()
        second_dir.mkdir()
        prompt = (
            "Work only inside the current directory. Append exactly one line alpha followed by a newline "
            "to artifact.txt, creating it if absent and preserving any existing content. "
            "Perform the file change using tools, then finish. Do not access credentials or other directories."
        )
        first, arguments = self.start(first_dir, prompt)
        repeated = self.client.tool("start_task", arguments)
        require(repeated["task_id"] == first["task_id"] and repeated["deduplicated"], "request retry created another task")
        self.expect_completed(first["task_id"])
        require((first_dir / "artifact.txt").read_text() == "alpha\n", "deduplicated task produced the wrong file content")
        self.report["checks"]["write_and_deduplicate"] = "passed"

        follow = self.client.tool("send_followup", {
            "request_id": str(uuid.uuid4()), "task_id": first["task_id"],
            "prompt": "Inside the same directory, append exactly one line beta followed by a newline to the same file you modified in the previous turn. Preserve its existing content. Perform the change using tools, then finish.",
        })
        self.tasks.append(follow["task_id"])
        require(follow["session_id"] == first["session_id"], "followup changed session")
        self.expect_completed(follow["task_id"])
        require((first_dir / "artifact.txt").read_text() == "alpha\nbeta\n", "followup did not preserve context and file content")
        self.client.tool("send_followup", {"request_id": str(uuid.uuid4()), "task_id": first["task_id"], "prompt": "Do nothing."}, expected_error="stale_task")
        self.report["checks"]["followup_and_stale_parent"] = "passed"

        a, _ = self.start(first_dir, "Work only in the current directory. Use tools to write isolation.txt with exactly worker-a followed by a newline, then finish. Do not access other directories or credentials.")
        b, _ = self.start(second_dir, "Work only in the current directory. Use tools to write isolation.txt with exactly worker-b followed by a newline, then finish. Do not access other directories or credentials.")
        require(a["session_id"] != b["session_id"], "independent tasks shared a session")
        self.expect_completed(a["task_id"])
        self.expect_completed(b["task_id"])
        require((first_dir / "isolation.txt").read_text() == "worker-a\n", "first directory received wrong result")
        require((second_dir / "isolation.txt").read_text() == "worker-b\n", "second directory received wrong result")
        self.report["checks"]["two_directories"] = "passed"

        # Terminal cancellation exercises the idempotent contract even without a writer run.
        cancelled = self.client.tool("cancel_task", {"task_id": b["task_id"]})
        require(cancelled["status"] == "completed", "terminal cancellation changed a completed task")
        self.report["checks"]["terminal_cancel"] = "passed"
        if self.args.test_cancel:
            self.exercise_cancel()
        else:
            self.report["checks"]["controlled_writer_cancel"] = "not_run"

        if not self.report["permission_requests_observed"]:
            self.client.tool("resolve_permission", {"task_id": b["task_id"], "permission_id": str(uuid.uuid4()), "option_id": "invalid"}, expected_error="permission_not_found")
            self.report["checks"]["real_permission_roundtrip"] = "not_observed; unknown-id error contract checked"
        else:
            self.report["checks"]["real_permission_roundtrip"] = "observed_and_resolved"
        self.close(follow["task_id"])
        self.client.tool("send_followup", {"request_id": str(uuid.uuid4()), "task_id": follow["task_id"], "prompt": "Do nothing."}, expected_error="session_closed")
        self.report["checks"]["close_and_reject_followup"] = "passed"

    def exercise_cancel(self):
        directory = self.root / "cancel"
        directory.mkdir()
        accepted, _ = self.start(directory, (
            "Work only inside this directory. Run a foreground Python writer that appends a line to writer.log, "
            "flushes it, and sleeps 0.1 seconds, repeating 300 times (30 seconds maximum). "
            "Do not background, detach, start a server, or launch extra worker processes. "
            "Wait for that foreground command; do not edit anything outside this directory or access credentials."
        ))
        task_id = accepted["task_id"]
        deadline = time.monotonic() + self.args.task_timeout
        while time.monotonic() < deadline:
            view = self.snapshot(task_id)
            output = directory / "writer.log"
            if output.exists() and output.stat().st_size > 0:
                break
            require(view["status"] not in TERMINAL, "writer ended before cancellation could be tested")
            time.sleep(0.1)
        else:
            raise AcceptanceError("controlled writer did not begin")
        self.client.tool("cancel_task", {"task_id": task_id})
        final = self.wait_terminal(task_id)
        require(final["status"] == "cancelled" and final["execution_state"] == "stopped", "controlled cancellation lacks stop evidence")
        content = output.read_bytes()
        for _ in range(4):
            time.sleep(0.5)
            require(output.read_bytes() == content, "controlled writer changed the file after cancellation")
        self.report["checks"]["controlled_writer_cancel"] = "passed; four stable samples over two seconds"

    def cleanup_sessions(self):
        confirmed = True
        by_session = {}
        for task_id in self.tasks:
            try:
                view = self.client.tool("get_task", {"task_id": task_id})
                if view["status"] not in TERMINAL:
                    self.client.tool("cancel_task", {"task_id": task_id})
                    view = self.wait_terminal(task_id, timeout=self.args.cleanup_timeout)
                if view["execution_state"] == "unknown":
                    confirmed = False
                by_session[view["session_id"]] = task_id
            except (AcceptanceError, OSError):
                confirmed = False
        for task_id in by_session.values():
            try:
                self.close(task_id, timeout=self.args.cleanup_timeout)
            except (AcceptanceError, OSError):
                confirmed = False
        return confirmed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bridge", type=Path, default=Path("target/debug/dsh-mcp"))
    parser.add_argument("--task-timeout", type=float, default=300)
    parser.add_argument("--request-timeout", type=float, default=15)
    parser.add_argument("--cleanup-timeout", type=float, default=80)
    parser.add_argument("--permission-policy", choices=["deny", "allow-once"], default="deny", help="only use an original one-time permission option; never grant persistent approval")
    parser.add_argument("--test-cancel", action="store_true", help="also run and cancel a bounded foreground writer")
    parser.add_argument("--keep", action="store_true", help="keep temporary files after successful confirmed cleanup")
    parser.add_argument("--report", type=Path, help="write the same credential-free JSON summary to this path")
    args = parser.parse_args()
    for value in (args.task_timeout, args.request_timeout, args.cleanup_timeout):
        if not 0 < value < float("inf"):
            parser.error("timeouts must be finite and positive")
    bridge = args.bridge.resolve()
    if not bridge.is_file():
        parser.error("bridge binary does not exist; build it first")
    root = Path(tempfile.mkdtemp(prefix="dsh-mcp-real-model-"))
    state_dir = root / "state"
    report = {"temporary_root": str(root), "checks": {}, "permission_requests_observed": 0, "permission_requests_resolved": 0, "cleanup_confirmed": False, "passed": False}
    client, acceptance = None, None
    error = None
    try:
        client = McpClient(bridge, state_dir, args.request_timeout)
        acceptance = Acceptance(client, root, args, report)
        client.initialize()
        report["checks"]["mcp_initialize_and_eleven_tools"] = "passed"
        acceptance.exercise()
        report["passed"] = True
    except (AcceptanceError, OSError, KeyError, ValueError, KeyboardInterrupt) as failure:
        error = type(failure).__name__ if not isinstance(failure, AcceptanceError) else str(failure)
        report["error"] = error
    finally:
        sessions_closed, shutdown = False, False
        try:
            sessions_closed = acceptance.cleanup_sessions() if acceptance is not None else True
        except (AcceptanceError, OSError, KeyError, ValueError, KeyboardInterrupt) as failure:
            report["session_cleanup_error"] = type(failure).__name__
        finally:
            try:
                shutdown = client.finish(args.cleanup_timeout) if client is not None else True
            except (OSError, ValueError, subprocess.TimeoutExpired, KeyboardInterrupt) as failure:
                report["shutdown_error"] = type(failure).__name__
        report["cleanup_confirmed"] = sessions_closed and shutdown and not (state_dir / "run-state.json").exists()
        report["passed"] = report["passed"] and report["cleanup_confirmed"]
        if report["passed"] and not args.keep:
            shutil.rmtree(root)
            report["temporary_files_removed"] = True
        else:
            report["temporary_files_removed"] = False
        encoded = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
        if args.report:
            args.report.write_text(encoded)
        sys.stdout.write(encoded)
    return 0 if report["passed"] and error is None else 1


if __name__ == "__main__":
    sys.exit(main())

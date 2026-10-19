#!/usr/bin/env python3
"""通过实际 Codex app-server 验证共享 MCP 接入，不启动任何模型轮次。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time
import tomllib

TOOLS = {
    'start_task', 'get_task', 'send_followup', 'cancel_task',
    'resolve_permission', 'close_session', 'list_sessions', 'resume_task',
    'list_tasks', 'attach_session', 'get_status',
}


def fingerprint(path):
    """只保存配置摘要，不输出配置或认证内容。"""
    return hashlib.sha256(path.read_bytes()).hexdigest()


class AppServer:
    """仅调用 Codex 控制接口，绝不发送 turn/start。"""

    def __init__(self, executable, overrides, cwd, log):
        """启动独立 stdio app-server，覆盖只作用于本进程。"""
        args = [executable]
        for override in overrides:
            args.extend(['-c', override])
        args.extend(['app-server', '--stdio'])
        self.process = subprocess.Popen(
            args, cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=log,
        )
        self.buffer = b''
        self.sequence = 0
        self.calls = []

    def request(self, method, params):
        """有界读取 JSON-RPC，只保留验收所需的返回值。"""
        assert method != 'turn/start'
        self.calls.append(method)
        self.sequence += 1
        seq = self.sequence
        self.process.stdin.write(json.dumps({
            'id': seq, 'method': method, 'params': params,
        }).encode() + b'\n')
        self.process.stdin.flush()
        deadline = time.monotonic() + 40
        while True:
            if b'\n' not in self.buffer:
                remaining = deadline - time.monotonic()
                assert remaining > 0 and select.select(
                    [self.process.stdout], [], [], max(0, remaining)
                )[0], f'Codex timeout: {method}'
                data = os.read(self.process.stdout.fileno(), 65536)
                assert data, f'Codex EOF: {method}'
                self.buffer += data
                assert len(self.buffer) <= 8 * 1024 * 1024
                continue
            line, self.buffer = self.buffer.split(b'\n', 1)
            reply = json.loads(line)
            if 'method' in reply and 'id' in reply:
                raise AssertionError(f'unexpected server request: {reply["method"]}')
            if reply.get('id') == seq:
                assert 'error' not in reply, reply.get('error')
                return reply['result']

    def tool(self, thread, name, expected_error=None, **arguments):
        """使用 Codex 原生 MCP 调用路径，检查结构化业务错误。"""
        response = self.request('mcpServer/tool/call', {
            'threadId': thread, 'server': 'dsh_acp', 'tool': name,
            'arguments': arguments,
        })
        body = response.get('structuredContent')
        assert isinstance(body, dict), f'{name}: structuredContent missing'
        if expected_error:
            assert response.get('isError') and body.get('code') == expected_error, body
        else:
            assert not response.get('isError'), body
        return body

    def close(self):
        """结束测试专属 app-server，不操作现有 Codex daemon。"""
        if not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.process.stdout.close()


def poll(probe, predicate):
    """轮询业务状态，避免把固定等待作为完成证据。"""
    deadline = time.monotonic() + 10
    while True:
        value = probe()
        if predicate(value):
            return value
        assert time.monotonic() < deadline, value
        time.sleep(0.03)


def verify(binary, codex, report_path):
    """真实 Codex 连接新 proxy，后端仅使用确定性 fixture。"""
    config = Path('/home/agent_coding/.codex/config.toml')
    launcher = Path('/home/agent_coding/.codex/tools/dsh-acp/launch.py')
    before = {str(p): fingerprint(p) for p in (config, launcher)}
    configured = tomllib.loads(config.read_text()).get('mcp_servers', {})
    report = {'passed': False, 'checks': [], 'model_turns': 0}
    with tempfile.TemporaryDirectory(prefix='dsh-codex-native-') as folder:
        root = Path(folder)
        runtime = root / 'runtime'
        runtime.mkdir(mode=0o700)
        state = root / 'state'
        socket = runtime / 'acp.sock'
        env = dict(os.environ,
            DSH_MCP_SOCKET=str(socket), DSH_MCP_STATE_DIR=str(state),
            DSH_MCP_DSH_COMMAND='/usr/bin/python3',
            DSH_MCP_DSH_ARGS_JSON=json.dumps([
                '-u', str(Path(__file__).resolve().parent.parent / 'tests/fixtures/fake_acp.py'),
            ]),
            DSH_MCP_FIXTURE_HISTORY='1', DSH_MCP_FIXTURE_HANG='',
            DSH_MCP_FIXTURE_STARTS=str(root / 'fixture-starts'),
        )
        # 日志仅写入本测试临时目录，不将全局配置或认证信息写入报告。
        with (root / 'daemon.log').open('wb') as daemon_log, (root / 'codex.log').open('wb') as codex_log:
            daemon = subprocess.Popen(
                [binary, '--daemon'], env=env, stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=daemon_log,
            )
            app = None
            try:
                deadline = time.monotonic() + 5
                while not socket.exists():
                    assert daemon.poll() is None, 'test daemon exited'
                    assert time.monotonic() < deadline
                    time.sleep(0.01)
                overrides = [
                    f'mcp_servers.dsh_acp.command={json.dumps(binary)}',
                    'mcp_servers.dsh_acp.args=["--proxy"]',
                    'mcp_servers.dsh_acp.enabled=true',
                    f'mcp_servers.dsh_acp.env.DSH_MCP_SOCKET={json.dumps(str(socket))}',
                    'mcp_servers.dsh_acp.startup_timeout_sec=10',
                    f'log_dir={json.dumps(str(root / "codex-logs"))}',
                ]
                for name in configured:
                    if name != 'dsh_acp':
                        overrides.append(f'mcp_servers.{json.dumps(name)}.enabled=false')
                app = AppServer(codex, overrides, root, codex_log)
                initialized = app.request('initialize', {
                    'clientInfo': {'name': 'dsh-native-acceptance', 'version': '1'},
                    'capabilities': {'experimentalApi': True},
                })
                report['codex_user_agent'] = initialized.get('userAgent')
                app.process.stdin.write(b'{"method":"initialized","params":{}}\n')
                app.process.stdin.flush()
                started = app.request('thread/start', {
                    'cwd': str(root), 'ephemeral': True,
                    'approvalPolicy': 'never', 'sandbox': 'read-only',
                })
                thread = started['thread']['id']
                inventory = app.request('mcpServerStatus/list', {
                    'threadId': thread, 'serverName': 'dsh_acp',
                })
                servers = inventory['data']
                server = next(s for s in servers if s['name'] == 'dsh_acp')
                names = set(server['tools'])
                assert names == TOOLS, {'actual_tools': sorted(names)}
                assert not server.get('toolsError'), server.get('toolsError')
                report['tool_names'] = sorted(names)
                report['runtime_status'] = server.get('runtimeStatus')
                report['checks'].append('codex_native_discovers_all_eleven_tools')
                status = app.tool(thread, 'get_status')
                assert status['backend']['state'] == 'dormant', status
                assert not (root / 'fixture-starts').exists()
                assert not app.tool(thread, 'list_tasks')['tasks']
                report['checks'].append('native_read_only_calls_do_not_start_acp')
                app.tool(thread, 'attach_session', expected_error='invalid_argument', session_id='invalid')
                report['checks'].append('native_structured_business_errors_preserved')
                history = app.tool(thread, 'list_sessions')
                assert history['sessions'][0]['dsh_session_id'] == 'history:alpha/1'
                resumed = app.tool(thread, 'resume_task', request_id='codex-native-resume',
                    dsh_session_id='history:alpha/1', cwd=str(root), prompt='hold')
                task = resumed['task_id']
                snapshot = poll(lambda: app.tool(thread, 'get_task', task_id=task),
                    lambda value: value['status'] == 'running')
                assert snapshot['resumed_from_dsh_session_id'] == 'history:alpha/1'
                discovered = app.tool(thread, 'list_tasks')
                assert discovered['tasks'][0]['owner_state'] == 'mine'
                app.tool(thread, 'cancel_task', task_id=task)
                poll(lambda: app.tool(thread, 'get_task', task_id=task),
                    lambda value: value['status'] == 'cancelled')
                app.tool(thread, 'close_session', task_id=task)
                poll(lambda: app.tool(thread, 'get_task', task_id=task),
                    lambda value: value['session']['state'] == 'closed')
                assert len((root / 'fixture-starts').read_text().splitlines()) == 1
                report['checks'].append('native_history_resume_query_cancel_close_with_fixture')
                report['codex_methods'] = sorted(set(app.calls))
            finally:
                if app is not None:
                    app.close()
                if daemon.poll() is None:
                    daemon.send_signal(signal.SIGTERM)
                    try:
                        daemon.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        daemon.kill()
                        daemon.wait()
                if sys.exc_info()[0] is not None:
                    for name in ['daemon.log', 'codex.log']:
                        evidence = report_path.with_suffix('.' + name)
                        with os.fdopen(os.open(evidence, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), 'wb') as stream:
                            stream.write((root / name).read_bytes())
            assert daemon.returncode == 0, 'test daemon cleanup unconfirmed'
            assert not socket.exists() and not (state / 'run-state.json').exists()
            report['checks'].append('test_processes_and_runtime_cleaned')
    for name, expected in before.items():
        assert fingerprint(Path(name)) == expected, f'production file changed: {name}'
    report['checks'].append('production_config_and_launcher_unchanged')
    report['passed'] = True
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report, ensure_ascii=False))


def main():
    """只接受显式 Codex、受测二进制和报告路径。"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--codex', required=True)
    parser.add_argument('--report', required=True, type=Path)
    args = parser.parse_args()
    verify(str(Path(args.binary).resolve()), str(Path(args.codex).resolve()), args.report)


if __name__ == '__main__':
    main()

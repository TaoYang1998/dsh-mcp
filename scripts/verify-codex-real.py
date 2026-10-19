#!/usr/bin/env python3
"""显式真实模型验收：Codex 原生接入、写文件、追加及跨 ACP 恢复。"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import tomllib

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('native_probe', Path(__file__).with_name('verify-codex-native.py'))
native = importlib.util.module_from_spec(spec)
spec.loader.exec_module(native)


class RealProbe:
    """持有测试专属 daemon 和 Codex，不启动 Codex 模型轮次。"""

    def __init__(self, root, binary, codex, configured, env):
        """保存独立状态目录与命令，仅复用已配置 DSH profile。"""
        self.root, self.binary, self.codex = root, binary, codex
        self.configured, self.env = configured, env
        self.daemon = self.app = self.thread = None
        self.logs = []
        self.generation = 0
        self.permissions = 0

    def start(self):
        """在相同稳定状态目录重启测试 daemon，再启动独立 Codex 客户端。"""
        self.generation += 1
        for component in ['daemon', 'codex']:
            self.logs.append((self.root / f'{component}-{self.generation}.log').open('wb'))
        self.daemon = subprocess.Popen([self.binary, '--daemon'], env=self.env,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=self.logs[-2])
        socket = Path(self.env['DSH_MCP_SOCKET'])
        deadline = time.monotonic() + 5
        while not socket.exists():
            assert self.daemon.poll() is None, 'test daemon exited early'
            assert time.monotonic() < deadline, 'test socket timeout'
            time.sleep(.01)
        overrides = [
            f'mcp_servers.dsh_acp.command={json.dumps(self.binary)}',
            'mcp_servers.dsh_acp.args=["--proxy"]',
            'mcp_servers.dsh_acp.enabled=true',
            f'mcp_servers.dsh_acp.env.DSH_MCP_SOCKET={json.dumps(str(socket))}',
            f'log_dir={json.dumps(str(self.root / "codex-logs"))}',
        ]
        for name in self.configured:
            if name != 'dsh_acp':
                overrides.append(f'mcp_servers.{json.dumps(name)}.enabled=false')
        self.app = native.AppServer(self.codex, overrides, self.root, self.logs[-1])
        self.app.request('initialize', {
            'clientInfo': {'name': 'dsh-real-native-acceptance', 'version': '1'},
            'capabilities': {'experimentalApi': True},
        })
        self.app.process.stdin.write(b'{"method":"initialized","params":{}}\n')
        self.app.process.stdin.flush()
        self.thread = self.app.request('thread/start', {
            'cwd': str(self.root), 'ephemeral': True,
            'approvalPolicy': 'never', 'sandbox': 'read-only',
        })['thread']['id']
        inventory = self.app.request('mcpServerStatus/list', {
            'threadId': self.thread, 'serverName': 'dsh_acp',
        })
        server = next(item for item in inventory['data'] if item['name'] == 'dsh_acp')
        assert set(server['tools']) == native.TOOLS and server.get('runtimeStatus') == 'connected'

    def tool(self, name, **args):
        """通过真实 Codex 的原生 MCP 路径调用工具。"""
        return self.app.tool(self.thread, name, **args)

    def completed(self, task):
        """等待真实轮次结束，权限仅选原始一次性允许选项。"""
        deadline = time.monotonic() + 180
        decisions = set()
        while time.monotonic() < deadline:
            view = self.tool('get_task', task_id=task)
            for permission in view.get('pending_permissions', []):
                pid = permission['permission_id']
                if pid in decisions:
                    continue
                option = next((item for item in permission['options'] if item['kind'] == 'allow_once'), None)
                assert option, 'no one-time permission option'
                self.tool('resolve_permission', task_id=task, permission_id=pid, option_id=option['option_id'])
                decisions.add(pid)
                self.permissions += 1
            if view['status'] in ['completed', 'failed', 'cancelled']:
                assert view['status'] == 'completed', {'status': view['status'], 'error': view.get('error')}
                assert view['execution_state'] == 'stopped'
                return view
            time.sleep(.25)
        raise AssertionError('real DSH task timeout')

    def close_session(self, task):
        """查询到 closed 后才认为真实会话清理完成。"""
        self.tool('close_session', task_id=task)
        native.poll(lambda: self.tool('get_task', task_id=task),
            lambda view: view['session']['state'] == 'closed')

    def stop(self):
        """正常关停测试进程，未知清理不自动确认恢复标记。"""
        if self.app is not None:
            self.app.close()
            self.app = None
        if self.daemon is not None:
            if self.daemon.poll() is None:
                self.daemon.send_signal(signal.SIGTERM)
                try:
                    self.daemon.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    self.daemon.kill()
                    self.daemon.wait()
            code = self.daemon.returncode
            self.daemon = None
            assert code == 0, 'daemon cleanup unconfirmed'
            assert not Path(self.env['DSH_MCP_STATE_DIR'], 'run-state.json').exists()
        for log in self.logs:
            log.close()
        self.logs = []


def verify(args):
    """真实文件由主验收程序核对，恢复回答不能重发随机代号。"""
    config_path = Path('/home/agent_coding/.codex/config.toml')
    launcher = Path('/home/agent_coding/.codex/tools/dsh-acp/launch.py')
    before = {str(p): native.fingerprint(p) for p in [config_path, launcher]}
    configured = tomllib.loads(config_path.read_text()).get('mcp_servers', {})
    settings = configured['dsh_acp'].get('env', {})
    for key in ['DSH_HOME', 'DSH_MCP_DSH_COMMAND', 'DSH_MCP_DSH_ARGS_JSON']:
        assert settings.get(key), f'configured {key} missing'
    root = Path(tempfile.mkdtemp(prefix='dsh-codex-real-'))
    runtime = root / 'runtime'
    runtime.mkdir(mode=0o700)
    cwd = root / 'workspace'
    cwd.mkdir(mode=0o700)
    env = dict(os.environ, **settings)
    env.update(DSH_MCP_SOCKET=str(runtime / 'acp.sock'), DSH_MCP_STATE_DIR=str(root / 'state'))
    for key in ['DSH_MCP_FIXTURE_HISTORY', 'DSH_MCP_FIXTURE_STARTS', 'DSH_MCP_FIXTURE_HANG']:
        env.pop(key, None)
    case = secrets.token_hex(8)
    context = secrets.token_hex(12)
    expected = f'dsh_mcp_real_probe={case}\n'.encode()
    report = {'passed': False, 'codex_model_turns': 0, 'dsh_model_turns': 0,
        'checks': [], 'root': str(root), 'production_entry_switched': False}
    probe = RealProbe(root, str(Path(args.binary).resolve()), str(Path(args.codex).resolve()), configured, env)
    try:
        probe.start()
        assert probe.tool('get_status')['backend']['state'] == 'dormant'
        report['checks'].append('real_codex_eleven_tools_connected')
        first = probe.tool('start_task', request_id=f'native-real-{case}', cwd=str(cwd),
            prompt=f'用户要求的真实验收。仅在当前临时目录创建 probe.txt，内容严格为 dsh_mcp_real_probe={case} 加一个换行。必须实际调用工具写入并检查，不修改其他文件，不读取凭据，不运行后台进程。另请在会话上下文中记住随机代号 {context}，不要把代号写入文件。最后简短报告完成。')
        view = probe.completed(first['task_id'])
        report['dsh_model_turns'] += 1
        assert (cwd / 'probe.txt').read_bytes() == expected
        assert sorted(p.name for p in cwd.iterdir()) == ['probe.txt']
        remote = view['session']['dsh_session_id']
        report.update(first_task_id=first['task_id'], dsh_session_id=remote,
            file_sha256=hashlib.sha256(expected).hexdigest())
        report['checks'].append('real_file_exact_bytes_verified')
        follow = probe.tool('send_followup', request_id=f'native-follow-{case}', task_id=first['task_id'],
            prompt='只回复我先前要求你保留在上下文里的随机代号，原样回复。不要读取文件，不要调用工具。')
        view = probe.completed(follow['task_id'])
        report['dsh_model_turns'] += 1
        assert view['result']['text'].strip() == context
        report['checks'].append('real_followup_context_exact_match')
        probe.close_session(follow['task_id'])
        probe.stop()
        report['checks'].append('first_acp_closed_and_daemon_cleanly_stopped')
        probe.start()
        probe.tool('get_task', expected_error='task_not_found', task_id=first['task_id'])
        page = probe.tool('list_sessions', cwd=str(cwd))
        found = any(item['dsh_session_id'] == remote for item in page['sessions'])
        while not found and page.get('next_cursor'):
            page = probe.tool('list_sessions', cwd=str(cwd), cursor=page['next_cursor'])
            found = any(item['dsh_session_id'] == remote for item in page['sessions'])
        assert found, 'closed history not listed'
        resumed = probe.tool('resume_task', request_id=f'native-resume-{case}', dsh_session_id=remote,
            cwd=str(cwd), prompt='回忆先前要求你只在上下文中保留的随机代号，只回复该代号。不要读取文件或调用工具。')
        view = probe.completed(resumed['task_id'])
        report['dsh_model_turns'] += 1
        assert resumed['task_id'] != first['task_id']
        assert resumed['session_id'] != first['session_id']
        assert view['session']['dsh_session_id'] == remote
        assert view['result']['text'].strip() == context
        assert (cwd / 'probe.txt').read_bytes() == expected
        report['checks'].append('real_cross_acp_resume_context_exact_match')
        report['resumed_task_id'] = resumed['task_id']
        probe.close_session(resumed['task_id'])
        report['checks'].append('resumed_session_confirmed_closed')
        report['passed'] = True
    except Exception as failure:
        report['error'] = str(failure)
        raise
    finally:
        try:
            probe.stop()
            report['cleanup_confirmed'] = True
        except Exception as failure:
            report['cleanup_confirmed'] = False
            report['passed'] = False
            report['cleanup_error'] = str(failure)
            raise
        finally:
            report['permissions_observed'] = probe.permissions
            report['production_files_unchanged'] = all(native.fingerprint(Path(name)) == digest for name, digest in before.items())
            if not report['production_files_unchanged']:
                report['passed'] = False
            args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report, ensure_ascii=False))


def main():
    """此入口会调用真实 DSH 模型，需显式指定二进制与报告。"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--codex', required=True)
    parser.add_argument('--report', required=True, type=Path)
    verify(parser.parse_args())


if __name__ == '__main__':
    main()

"""用临时目录和确定性 ACP 验证真实共享传输，不调用模型。"""
import argparse
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import tempfile
import time


class Client:
    """一个客户端始终使用独立 MCP initialize 与请求编号。"""
    def __init__(self, binary, env):
        """启动独立代理并完成本连接的 MCP 握手。"""
        self.process = subprocess.Popen([binary, '--proxy'], env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.seq = 0
        self.buf = b''
        self.request('initialize', {'protocolVersion': '2025-06-18', 'capabilities': {}, 'clientInfo': {'name': 'shared-probe', 'version': '1'}})
        self.process.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        self.process.stdin.flush()

    def request(self, method, params):
        """有界等待单个响应，stdout 必须保持纯 JSON。"""
        self.seq += 1
        self.process.stdin.write(json.dumps({'jsonrpc': '2.0', 'id': self.seq, 'method': method, 'params': params}).encode() + b'\n')
        self.process.stdin.flush()
        deadline = time.monotonic() + 8
        while True:
            if b'\n' not in self.buf:
                remaining = deadline - time.monotonic()
                assert remaining > 0 and select.select([self.process.stdout], [], [], remaining)[0], 'MCP response timeout'
                data = os.read(self.process.stdout.fileno(), 65536)
                assert data, 'MCP EOF: ' + self.process.stderr.read().decode()
                self.buf += data
                continue
            line, self.buf = self.buf.split(b'\n', 1)
            value = json.loads(line)
            if value.get('id') == self.seq:
                assert 'error' not in value, value
                return value['result']

    def tool(self, name, **args):
        """返回结构化业务结果，错误由场景自行断言。"""
        result = self.request('tools/call', {'name': name, 'arguments': args})
        return result['structuredContent']

    def close(self):
        """只结束本客户端，daemon 必须继续运行。"""
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.process.stdout.close()
        self.process.stderr.close()


def wait_until(probe, predicate, timeout=8):
    """按状态轮询，有界失败，不把固定等待当完成证据。"""
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if predicate(value):
            return value
        assert time.monotonic() < deadline, value
        time.sleep(.01)


def verify(binary):
    """验证客户端独立、全局额度、断线接管、恢复与超限隔离。"""
    report = []
    with tempfile.TemporaryDirectory(prefix='dsh-shared-probe-') as folder:
        root = Path(folder)
        runtime = root / 'runtime'
        runtime.mkdir(mode=0o700)
        state = root / 'state'
        env = dict(os.environ, DSH_MCP_SOCKET=str(runtime / 'acp.sock'), DSH_MCP_STATE_DIR=str(state),
                   DSH_MCP_DSH_COMMAND='python3', DSH_MCP_DSH_ARGS_JSON=json.dumps(['-u', str(Path(__file__).resolve().parent.parent / 'tests/fixtures/fake_acp.py')]),
                   DSH_MCP_FIXTURE_HISTORY='1', DSH_MCP_FIXTURE_HANG='', DSH_MCP_FIXTURE_STARTS=str(root / 'starts'))
        log = (root / 'daemon.log').open('wb')
        daemon = subprocess.Popen([binary, '--daemon'], env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=log)
        clients = []
        try:
            deadline=time.monotonic()+5
            while not Path(env['DSH_MCP_SOCKET']).exists():
                assert daemon.poll() is None, (root / 'daemon.log').read_text()
                assert time.monotonic()<deadline, 'socket not created'
                time.sleep(.01)
            a=Client(binary,env);b=Client(binary,env);clients.extend([a,b])
            assert len(a.request('tools/list',{})['tools']) == 11
            assert b.tool('get_status')['backend']['state']=='dormant'
            assert not (root/'starts').exists()
            report.append('two_independent_proxies_no_eager_acp')
            tasks=[]
            for index in range(4):
                owner=a if index%2==0 else b
                result=owner.tool('start_task',request_id=f'task-{index}',cwd=str(root),prompt='hold')
                assert 'task_id' in result,result
                tasks.append((owner,result))
            assert b.tool('start_task',request_id='overflow',cwd=str(root),prompt='hold')['code']=='capacity_exceeded'
            for owner,result in tasks:
                wait_until(lambda:owner.tool('get_task',task_id=result['task_id']),lambda v:v['status']=='running')
            assert len((root/'starts').read_text().splitlines())==1
            report.append('one_acp_four_global_tasks_fifth_rejected')
            first=tasks[0][1]
            assert b.tool('cancel_task',task_id=first['task_id'])['code']=='session_not_owned'
            assert b.tool('attach_session',session_id=first['session_id'])['code']=='session_owned'
            a.close();clients.remove(a)
            wait_until(lambda:b.tool('list_tasks',scope='unowned'),lambda v:len(v['tasks'])==2)
            assert b.tool('get_task',task_id=tasks[1][1]['task_id'])['status']=='running'
            c=Client(binary,env);clients.append(c)
            c.tool('attach_session',session_id=first['session_id'])
            c.tool('cancel_task',task_id=first['task_id'])
            wait_until(lambda:c.tool('get_task',task_id=first['task_id']),lambda v:v['status']=='cancelled')
            report.append('disconnect_keeps_work_explicit_attach_controls_it')
            for owner,result in tasks[1:]:
                owner=b if owner is b else c
                if result['session_id']!=first['session_id'] and owner is c:
                    c.tool('attach_session',session_id=result['session_id'])
                owner.tool('cancel_task',task_id=result['task_id'])
                wait_until(lambda:owner.tool('get_task',task_id=result['task_id']),lambda v:v['status']=='cancelled')
                owner.tool('close_session',task_id=result['task_id'])
                wait_until(lambda:owner.tool('get_task',task_id=result['task_id']),lambda v:v['session']['state']=='closed')
            c.tool('close_session',task_id=first['task_id'])
            wait_until(lambda:c.tool('get_task',task_id=first['task_id']),lambda v:v['session']['state']=='closed')
            page=b.tool('list_sessions')
            assert page['sessions'] and page['sessions'][0]['dsh_session_id']=='history:alpha/1',page
            resumed=b.tool('resume_task',request_id='resume',dsh_session_id='history:alpha/1',cwd=str(root),prompt='hold')
            snap=wait_until(lambda:b.tool('get_task',task_id=resumed['task_id']),lambda v:v['status']=='running')
            assert snap['resumed_from_dsh_session_id']=='history:alpha/1'
            b.tool('cancel_task',task_id=resumed['task_id'])
            wait_until(lambda:b.tool('get_task',task_id=resumed['task_id']),lambda v:v['status']=='cancelled')
            b.tool('close_session',task_id=resumed['task_id'])
            wait_until(lambda:b.tool('get_task',task_id=resumed['task_id']),lambda v:v['session']['state']=='closed')
            report.append('real_acp_history_wire_and_resume')
            raw=socket.socket(socket.AF_UNIX);raw.connect(env['DSH_MCP_SOCKET']);raw.settimeout(3)
            try:
                raw.sendall(b'x'*(1024*1024+1)+b'\n')
                assert raw.recv(1)==b''
            except (BrokenPipeError,ConnectionResetError):
                pass
            finally:
                raw.close()
            assert b.tool('get_status')['backend']['state']=='ready'
            report.append('oversized_frame_only_disconnects_its_client')
            # 两个存活客户端加三十个未初始化连接耗尽默认三十二个连接额度。
            idle=[]
            try:
                for _ in range(30):
                    connection=socket.socket(socket.AF_UNIX);connection.connect(env['DSH_MCP_SOCKET']);idle.append(connection)
                extra=socket.socket(socket.AF_UNIX);extra.settimeout(3);extra.connect(env['DSH_MCP_SOCKET'])
                try:
                    assert extra.recv(1)==b''
                finally:
                    extra.close()
                assert b.tool('get_status')['backend']['state']=='ready'
                # 未初始化连接超过十秒后会被服务端释放。
                idle[0].settimeout(12)
                assert idle[0].recv(1)==b''
            finally:
                for connection in idle:connection.close()
            report.append('connection_cap_includes_uninitialized_clients_and_handshakes_expire')
            # 第二实例不能移除正在使用的 socket。
            second=subprocess.run([binary,'--daemon'],env=env,stdin=subprocess.DEVNULL,capture_output=True,timeout=3)
            assert second.returncode!=0
            assert b.request('ping',{})=={}
            report.append('second_daemon_cannot_replace_live_socket')
        finally:
            for client in clients:
                client.close()
            if daemon.poll() is None:
                daemon.send_signal(signal.SIGTERM)
                try:
                    daemon.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    daemon.kill();daemon.wait()
            log.close()
        assert daemon.returncode==0,(root/'daemon.log').read_text()
        assert not (state/'run-state.json').exists()
        assert not Path(env['DSH_MCP_SOCKET']).exists()
        report.append('normal_shutdown_cleans_owned_socket_and_marker')
    print(json.dumps({'passed':True,'checks':report}))


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',required=True);args=parser.parse_args()
    verify(str(Path(args.binary).resolve()))

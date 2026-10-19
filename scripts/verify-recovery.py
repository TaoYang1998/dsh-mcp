"""真实传输验证恢复保护；仅操作本脚本启动的无执行后代 fixture。"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import sys
sys.dont_write_bytecode=True
import time
from importlib.util import spec_from_file_location, module_from_spec
spec=spec_from_file_location('shared_probe',Path(__file__).with_name('verify-shared-daemon.py'))
shared=module_from_spec(spec)
spec.loader.exec_module(shared)


def verify(binary):
    """后端隔离后先核对清理，再人工确认和重启，禁止自动继续旧任务。"""
    with tempfile.TemporaryDirectory(prefix='dsh-recovery-probe-') as folder:
        root=Path(folder);runtime=root/'runtime';runtime.mkdir(mode=0o700)
        state=root/'state'
        env=dict(os.environ,DSH_MCP_SOCKET=str(runtime/'acp.sock'),DSH_MCP_STATE_DIR=str(state),DSH_MCP_DSH_COMMAND='python3',
                 DSH_MCP_DSH_ARGS_JSON=json.dumps(['-u',str(Path(__file__).resolve().parent.parent/'tests/fixtures/fake_acp.py')]),
                 DSH_MCP_FIXTURE_HISTORY='1',DSH_MCP_FIXTURE_STARTS=str(root/'starts'),DSH_MCP_FIXTURE_HANG='')
        children=[];clients=[]
        def start():
            """等待可用 socket 而非仅等待进程启动。"""
            child=subprocess.Popen([binary,'--daemon'],env=env,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            children.append(child)
            deadline=time.monotonic()+5
            while not Path(env['DSH_MCP_SOCKET']).exists():
                assert child.poll() is None,'daemon exited early'
                assert time.monotonic()<deadline
                time.sleep(.01)
            client=shared.Client(binary,env);clients.append(client)
            return child,client
        def stop(child,client):
            """只关闭当前测试连接及对应守护进程。"""
            client.close();clients.remove(client)
            child.send_signal(signal.SIGTERM);child.wait(timeout=10)
        try:
            daemon,a=start()
            out=a.tool('start_task',request_id='isolation',cwd=str(root),prompt='hold')
            shared.wait_until(lambda:a.tool('get_task',task_id=out['task_id']),lambda v:v['status']=='running')
            peer=int((root/'starts').read_text().strip())
            # fixture 从不派生执行后代，杀死此已知 PID 可模拟 ACP 故障。
            os.kill(peer,signal.SIGTERM)
            snap=shared.wait_until(lambda:a.tool('get_task',task_id=out['task_id']),lambda v:v['backend']['state']=='unavailable')
            assert snap['session']['state']=='lost' and snap['backend']['recovery_required']
            assert a.tool('resume_task',request_id='blocked',dsh_session_id='history:alpha/1',cwd=str(root),prompt='hold')['code']=='backend_unavailable'
            stop(daemon,a)
            assert (state/'run-state.json').exists()
            daemon,a=start()
            assert a.tool('get_status')['backend']['recovery_required']
            assert a.tool('start_task',request_id='still-blocked',cwd=str(root),prompt='hold')['code']=='recovery_required'
            assert len((root/'starts').read_text().splitlines())==1
            stop(daemon,a)
            # 确定性 fixture 已退出且没有执行后代，仅在这个临时目录确认清理。
            ack=subprocess.run([binary,'--acknowledge-cleanup'],env=env,capture_output=True,timeout=3)
            assert ack.returncode==0,ack.stderr.decode()
            daemon,a=start()
            assert a.tool('get_status')['backend']['state']=='dormant'
            resumed=a.tool('resume_task',request_id='after-ack',dsh_session_id='history:alpha/1',cwd=str(root),prompt='hold')
            shared.wait_until(lambda:a.tool('get_task',task_id=resumed['task_id']),lambda v:v['status']=='running')
            a.tool('cancel_task',task_id=resumed['task_id'])
            shared.wait_until(lambda:a.tool('get_task',task_id=resumed['task_id']),lambda v:v['status']=='cancelled')
            a.tool('close_session',task_id=resumed['task_id'])
            shared.wait_until(lambda:a.tool('get_task',task_id=resumed['task_id']),lambda v:v['session']['state']=='closed')
            stop(daemon,a)
            assert daemon.returncode==0
            assert not (state/'run-state.json').exists()
            print(json.dumps({'passed':True,'checks':['backend_isolation_visible','no_resume_while_unavailable','restart_does_not_clear_recovery','manual_ack_after_fixture_cleanup','explicit_resume_creates_new_bridge_task']}))
        finally:
            for client in clients:client.close()
            for child in children:
                if child.poll() is None:
                    child.kill();child.wait()


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--binary',required=True);args=parser.parse_args();verify(str(Path(args.binary).resolve()))

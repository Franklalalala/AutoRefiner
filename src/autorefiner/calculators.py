import os
import time
from pathlib import Path
from typing import List, Union, Optional
import subprocess
import shutil

from dflow import Step, Workflow, download_artifact, upload_artifact
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate



class simpleExe(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'workbase': Artifact(Path),
            'cmd_list': List[str],
            'out_list': List[str],
            'in_fmt': str,
            'log_file': str,
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'outs': Artifact(type=List[Path]),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        cwd_ = os.getcwd()
        os.chdir(op_in['workbase'])
        cmd_list = op_in['cmd_list']
        outs = []
        if op_in['in_fmt'] == 'null':
            cmd_list.append(os.listdir('./')[0])
        else:
            for a_file in os.listdir('./'):
                if a_file.endswith(op_in['in_fmt']):
                    cmd_list.append(a_file)
                    break

        log_file = op_in['log_file']
        cmd_list.append(f'>>{log_file}')
        os.system(' '.join(cmd_list))

        for an_output in op_in['out_list']:
            if os.path.exists(an_output):
                outs.append(Path(os.path.join(op_in['workbase'], an_output)))
        os.chdir(cwd_)
        op_out = OPIO({
            "outs": outs,
        })
        return op_out


class batchExe(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_cooking': Artifact(Path),
            'in_para': dict
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_cooking': Artifact(type=List[Path]),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        def _stack_a_job():
            os.chdir(job_list[tracker])
            if 'in_fmt' in in_para.keys():
                for an_input in os.listdir('./'):
                    if an_input.endswith(in_para['in_fmt']):
                        cmd_list.append(an_input)
                        break
            else:
                cmd_list.append(os.listdir('./')[0])
            if 'log_file' in in_para.keys():
                log_str = '>>' + in_para['log_file']
                cmd_list.append(log_str)

            cmd_line = ' '.join(cmd_list)
            start_node = in_para['base_node'] + in_para['cpu_per_worker'] * worker_id
            end_node = in_para['base_node'] + in_para['cpu_per_worker'] * (worker_id + 1) - 1

            res_list[worker_id] = subprocess.Popen(f'taskset -c {start_node}-{end_node} {cmd_line}', shell=True)

            del cmd_list[-1]
            if 'log_file' in in_para.keys():
                del cmd_list[-1]
            os.chdir('./..')

        in_para = op_in['in_para']

        cmd_list = in_para['cmd_list']
        cwd_ = os.getcwd()
        os.chdir(op_in['in_cooking'])
        tracker = 0
        job_list = os.listdir('./')
        job_num = len(job_list)
        res_list = [None] * in_para['num_worker']
        while tracker < job_num:
            worker_id = tracker
            if os.path.isdir(job_list[tracker]):
                _stack_a_job()
            tracker = tracker + 1
            if worker_id == in_para['num_worker'] - 1:
                break

        while tracker < job_num:
            for worker_id, a_proc in enumerate(res_list):
                if tracker < job_num:
                    if a_proc.poll() == 0:
                        _stack_a_job()
                        tracker = tracker + 1
                else:
                    break
            if 'poll_interval' in in_para.keys():
                time.sleep(in_para['poll_interval'])

        for a_proc in res_list:
            a_proc.wait()
        outs = []
        cwd_2 = os.getcwd()
        for a_job in job_list:
            os.chdir(cwd_2)
            os.chdir(a_job)
            for an_output in os.listdir('./'):
                for a_fmt in in_para['out_list']:
                    if an_output.endswith(a_fmt):
                        outs.append(Path(os.path.join(op_in['in_cooking'], a_job, an_output)))
                        break
        os.chdir(cwd_)
        op_out = OPIO({
            "out_cooking": outs,
        })
        return op_out


class asePreSan(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'workbase': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_cooked': Artifact(Path),
            'info': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        from ase.calculators.emt import EMT
        from ase.io import read, write
        import pandas as pd

        cwd_ = os.getcwd()
        os.chdir(op_in['workbase'])
        name_list = []
        e_list = []
        for a_file in os.listdir('./'):
            file_name = os.path.splitext(a_file)[0]
            name_list.append(file_name)
            an_atoms = read(a_file)
            an_atoms.calc = EMT()
            e_list.append(an_atoms.get_potential_energy())
        os.chdir(cwd_)
        info = pd.DataFrame({'name': name_list, 'e': e_list})
        info = info.sort_values(by='e')
        info.index = sorted(info.index)
        info.to_pickle('info.pickle')
        os.rename(src=op_in['workbase'], dst='cooked')
        op_out = OPIO({
            'out_cooked': Path('cooked'),
            'info': Path('info.pickle')
        })
        return op_out








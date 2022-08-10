import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from autorefiner.refiners import xTB_Refiner
from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices

upload_packages.append(r'H:\dflow\AutoRefiner\src\autorefiner')
upload_packages.append(r'D:\Anaconda\envs\dflow\Lib\site-packages\ase')

dispatcher_executor = DispatcherExecutor(
    image="python_diy_5:3.8",
    machine_dict={
        'remote_root': r'/home/mkliu/test_dpdispatcher/',
        'batch_type': 'Torque',
        'remote_profile': {
            "hostname": "",
            "username": "",
            "password": "",
            'port': 22
        }
    },
    resources_dict={
        'number_node': 1,
        'cpu_per_node': 48,
        'gpu_per_node': 0,
        'group_size': 5,
        'queue_name': 'batch',
        'envs': {
            "OMP_STACKSIZE": "4G",
            "OMP_NUM_THREADS": "3,1",
            "OMP_MAX_ACTIVE_LEVELS": "1",
            "MKL_NUM_THREADS": "3",
            "DFLOW_WORKFLOW": "{{workflow.name}}",
            "DFLOW_POD": "{{pod.name}}"
        },
    }
)

if __name__ == "__main__":
    src_dir = upload_artifact('./input_files/batch_raw_xyz')
    info = upload_artifact('./input_files/info.pickle')

    xtb_template = xTB_Refiner(public_in_para={'num_worker': 3,
                                               'cpu_per_worker': 12,
                                               'base_node': 0,
                                               'poll_interval': 0.5,
                                               'cmd_list': ['/home/mkliu/anaconda3/envs/env001/bin/xtb', '--opt tight'],
                                               'out_list': ['xtbopt.log', 'xtbopt.xyz'],
                                               },
                               cutoff_para={'mode': 'rank', 'rank': 5},
                               image="python_diy_5:3.8",
                               dispatcher_executor=dispatcher_executor,
                               name='xtb-templates'
                               )

    xtb_ref = Step(name='test001',
                   template=xtb_template,
                   artifacts={'init': src_dir, 'info': info},
                   )

    wf = Workflow(id='wf002-hqxtd')
    step_input = wf.query_step(name='inputGen')[0]
    step_build = wf.query_step(name='build')[0]
    step_exe = wf.query_step(name='exe')[0]

    wf = Workflow(name="wf003")
    wf.add(xtb_ref)
    wf.submit(reuse_step=[step_input, step_build, step_exe])
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="test001")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/')

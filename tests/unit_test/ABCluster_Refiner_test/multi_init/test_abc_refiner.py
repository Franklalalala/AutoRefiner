import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from autorefiner.refiners import ABC_Refiner
from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices

upload_packages.append(r'H:\dflow\AutoRefiner\src\autorefiner')

dispatcher_executor = DispatcherExecutor(
    image="python_diy_5:3.8",
    machine_dict={
        'remote_root': r'/home/mkliu/test_dpdispatcher/',
        'batch_type': 'Torque',
        'remote_profile': {
            "hostname": "219.245.39.76",
            "username": "mkliu",
            "password": "mkliu123",
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
    src_dir = upload_artifact('./input_files/hand_tuned_structures')
    public_inp = upload_artifact('./input_files/c6h12.inp')

    local_minima_folder = 'job1'
    ABC_template = ABC_Refiner(public_in_para={'num_worker': 3,
                                               'cpu_per_worker': 12,
                                               'base_node': 0,
                                               'poll_interval': 0.5,
                                               'cmd_list': ['/home/mkliu/polymer/ABCluster/geom'],
                                               'out_list': ['info.out', local_minima_folder],
                                               'in_fmt': 'inp',
                                               'log_file': 'info.out'
                                               },
                               image="python_diy_5:3.8",
                               dispatcher_executor=dispatcher_executor,
                               local_minima_folder=local_minima_folder,
                               name='test001',
                               )

    abc_ref = Step(name='tests',
                   template=ABC_template,
                   artifacts={'init': src_dir, 'public_inp': public_inp},
                   )

    wf = Workflow(id='wf003-cc7nq')
    step_in = wf.query_step(name="inputGen")[0]
    step_build = wf.query_step(name="build")[0]
    step_exe = wf.query_step(name="exe")[0]

    wf = Workflow(name='wf004')
    wf.add(abc_ref)
    wf.submit(reuse_step=[step_in,step_build,step_exe])

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="tests")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/')



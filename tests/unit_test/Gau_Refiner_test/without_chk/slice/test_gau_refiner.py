import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from autorefiner.refiners import Gau_Refiner
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
    src_dir = upload_artifact('./input_files/batch_raw_xyz')
    info = upload_artifact('./input_files/info.pickle')

    gau_template = Gau_Refiner(public_in_para={'num_worker': 3,
                                               'cpu_per_worker': 12,
                                               'base_node': 0,
                                               'poll_interval': 0.5,
                                               'mem_per_worker': '10GB',
                                               'cmd_list': ['g16'],
                                               'out_list': ['log', 'chk'],
                                               },
                               cutoff_para={
                                   'mode': 'rank',
                                   'rank': 5
                               },
                               image="python_diy_5:3.8",
                               dispatcher_executor=dispatcher_executor,
                               name='gau-templates',
                               has_slice=True,
                               cmd_line_list=[r'opt b3lyp/6-311G(d, p) empiricaldispersion=GD3', r'opt b3lyp/6-31G empiricaldispersion=GD3'],
                               prefix_list=['6-311G-d-p', '6-31G']
                               )

    gau_ref = Step(name='gau-ref',
                   template=gau_template,
                   artifacts={'init': src_dir, 'info': info},
                   parameters={
                               'charge': 0,
                               'multi': 1,
                               'prefix': "{{item}}",
                               },
                   with_param=['6-311G-d-p', '6-31G']
                   )

    wf = Workflow(name="wf002")
    wf.add(gau_ref)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end_0 = wf.query_step(name="gau-ref")[0]

    assert (step_end_0.phase == "Succeeded")

    download_artifact(artifact=step_end_0.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end_0.outputs.artifacts['info'], path=r'./output_files/')
    step_end_1 = wf.query_step(name="gau-ref")[1]
    download_artifact(artifact=step_end_1.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end_1.outputs.artifacts['info'], path=r'./output_files/')



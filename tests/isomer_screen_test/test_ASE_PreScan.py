import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from autorefiner.calculators import asePreSan
from dflow import Step, Workflow, download_artifact, upload_artifact
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages

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
            "DFLOW_WORKFLOW": "{{workflow.name}}",
            "DFLOW_POD": "{{pod.name}}"
        },
    }
)


if __name__ == "__main__":
    src_dir = upload_artifact('./input_files/batch_raw_xyz')

    ase_prescan = Step(name='ase-prescan',
                   template=PythonOPTemplate(op_class=asePreSan, image="python_diy_5:3.8"),
                   artifacts={'workbase': src_dir},
                       executor=dispatcher_executor
                   )

    wf = Workflow(name="wf002")
    wf.add(ase_prescan)
    wf.submit()
    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end_0 = wf.query_step(name="ase-prescan")[0]

    assert (step_end_0.phase == "Succeeded")

    download_artifact(artifact=step_end_0.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end_0.outputs.artifacts['info'], path=r'./output_files/')




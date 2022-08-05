import os
import time
from pathlib import Path
from typing import List
import subprocess
import shutil
import time
from dflow import Step, Workflow, download_artifact, upload_artifact, argo_range
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate, upload_packages, Slices
from autorefiner.calculators import simpleExe
from autorefiner.parser import abcParser
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
        'cpu_per_node': 3,
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
    wf = Workflow(name="wf002")

    artifact0 = upload_artifact(Path("./init_c6h12"))
    # The folder name is pre-defined in the input file.(./init_c6h12/c6h12.inp)
    local_minima_folder = 'job1'
    step_exe = Step(
        name="exe",
        template=PythonOPTemplate(simpleExe, image="python_diy_5:3.8"),
        artifacts={"workbase": artifact0},
        parameters={'cmd_list': ['/home/mkliu/polymer/ABCluster/geom'], 'out_list': ['info.out', local_minima_folder],
                    'in_fmt': 'inp', 'log_file': 'info.out'},
        executor=dispatcher_executor,
    )
    step_parse = Step(
        name='parse',
        template=PythonOPTemplate(abcParser, image="python_diy_5:3.8"),
        artifacts={'in_cooked': step_exe.outputs.artifacts['outs']},
        parameters={'local_minima_folder': local_minima_folder, 'prefix': 'test001'}
    )
    wf.add(step_exe)
    wf.add(step_parse)
    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="parse")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files')


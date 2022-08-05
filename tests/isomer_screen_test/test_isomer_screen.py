import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import List

from autorefiner.calculators import asePreSan
from autorefiner.refiners import xTB_Refiner, Gau_Refiner
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
            "DFLOW_WORKFLOW": "{{workflow.name}}",
            "DFLOW_POD": "{{pod.name}}"
        },
    }
)

xtb_template = xTB_Refiner(public_in_para={'num_worker': 4,
                                           'cpu_per_worker': 12,
                                           'base_node': 0,
                                           'poll_interval': 0.5,
                                           'cmd_list': ['/home/mkliu/anaconda3/envs/env001/bin/xtb', '--opt tight'],
                                           'out_list': ['xtbopt.log', 'xtbopt.xyz'],
                                           },
                           cutoff_para={'mode': 'rank', 'rank': 6},
                           image="python_diy_5:3.8",
                           dispatcher_executor=dispatcher_executor,
                           name='xtb-tem'
                           )
gau_template_opt = Gau_Refiner(public_in_para={'num_worker': 2,
                                               'cpu_per_worker': 24,
                                               'base_node': 0,
                                               'poll_interval': 0.5,
                                               'mem_per_worker': '10GB',
                                               'cmd_list': ['g16'],
                                               'out_list': ['log', 'chk'],
                                               },
                               cutoff_para={'mode': 'rank', 'rank': 4},
                               image="python_diy_5:3.8",
                               dispatcher_executor=dispatcher_executor,
                               name='gau-0-tem',
                               keep_chk=True,
                               )

if __name__ == "__main__":
    wf = Workflow(name='isomer-screen')

    src_dir = upload_artifact('./input_files/batch_raw_xyz')
    ase_prescan = Step(name='ase-prescan',
                       template=PythonOPTemplate(op_class=asePreSan, image="python_diy_5:3.8"),
                       artifacts={'workbase': src_dir},
                       executor=dispatcher_executor
                       )
    wf.add(ase_prescan)
    xtb_ref = Step(name='xtb',
                   template=xtb_template,
                   artifacts={'init': ase_prescan.outputs.artifacts['out_cooked'],
                              'info': ase_prescan.outputs.artifacts['info']},
                   )
    wf.add(xtb_ref)

    gau_opt = Step(name='gau-opt',
                   template=gau_template_opt,
                   artifacts={'init': xtb_ref.outputs.artifacts['out_cooked'],
                              'info': xtb_ref.outputs.artifacts['info']},
                   parameters={'cmd_line': r' opt b3lyp/6-31G',
                               'charge': 0,
                               'multi': 1,
                               'prefix': 'opted'}
                   )
    wf.add(gau_opt)

    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(1)

    assert (wf.query_status() == "Succeeded")
    step_end = wf.query_step(name="gau-opt")[0]

    assert (step_end.phase == "Succeeded")

    download_artifact(artifact=step_end.outputs.artifacts['out_cooked'], path=r'./output_files/')
    download_artifact(artifact=step_end.outputs.artifacts['info'], path=r'./output_files/')
    download_artifact(artifact=step_end.outputs.artifacts['cooked_chk'], path=r'./output_files/')

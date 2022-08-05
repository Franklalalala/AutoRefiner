import os
import time
from pathlib import Path
from typing import List, Optional, Union
import subprocess
import shutil

from dflow import Step, Workflow, download_artifact, upload_artifact
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import OP, OPIO, Artifact, OPIOSign, PythonOPTemplate
import pandas as pd


class simpleBuild(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_workbase': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_workbase': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        name = 'cooking'
        cwd_ = os.getcwd()
        dst = os.path.abspath(name)
        os.makedirs(dst, exist_ok=True)
        if os.path.isdir(os.path.join(op_in['in_workbase'], 'raw')):
            raw = os.path.abspath(os.path.join(op_in['in_workbase'], 'raw'))
        else:
            raw = os.path.abspath(op_in['in_workbase'])
        for a_file in os.listdir(raw):
            os.chdir(dst)
            a_file_name = os.path.splitext(a_file)[0]
            os.makedirs(a_file_name, exist_ok=True)
            shutil.copy(src=os.path.join(raw, a_file), dst=os.path.join(a_file_name, a_file))
        os.chdir(cwd_)
        op_out = OPIO({
            "out_workbase": Path(name),
        })
        return op_out

# Build with auxiliary files.
# if is_private is true, files in in_aux folder should contain identical names with in_workbase folder
# if false, it should be comman files(repeated in every workbase)
class BuildWithAux(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_workbase': Artifact(Path),
            'in_aux': Artifact(Path),
            'is_private': bool
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_workbase': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        name = 'cooking'
        cwd_ = os.getcwd()
        dst = os.path.abspath(name)
        os.makedirs(dst, exist_ok=True)

        raw = os.path.abspath(op_in['in_workbase'])
        aux_path = os.path.abspath(op_in['in_aux'])
        if op_in['is_private']:
            aux_fmt = os.path.splitext(os.listdir(aux_path)[0])[1]
            for a_file in os.listdir(raw):
                os.chdir(dst)
                a_file_name = os.path.splitext(a_file)[0]
                os.makedirs(a_file_name, exist_ok=True)
                os.chdir(a_file_name)
                shutil.copy(src=os.path.join(aux_path, a_file_name + aux_fmt), dst=a_file_name + aux_fmt)
                shutil.copy(src=os.path.join(raw, a_file), dst=a_file)
        else:
            for a_file in os.listdir(raw):
                os.chdir(dst)
                a_file_name = os.path.splitext(a_file)[0]
                os.makedirs(a_file_name, exist_ok=True)
                os.chdir(a_file_name)
                for a_public_file in os.listdir(aux_path):
                    shutil.copy(src=os.path.join(aux_path, a_public_file), dst=a_public_file)
                shutil.copy(src=os.path.join(raw, a_file), dst=a_file)
        # To do: mixed senerio
        os.chdir(cwd_)
        op_out = OPIO({
            "out_workbase": Path(name),
        })
        return op_out


class Empty(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_s': str
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_s': str
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        op_out = OPIO({
            "out_s": op_in['in_s'],
        })
        return op_out


class simpleParaSlice(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'prefix': str,
            'cmd_line_list': list,
            'prefix_list': list
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'cmd_line': str,
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        slice_idx = op_in['prefix_list'].index(op_in['prefix'])
        op_out = OPIO({
            'cmd_line': op_in['cmd_line_list'][slice_idx]
        })
        return op_out


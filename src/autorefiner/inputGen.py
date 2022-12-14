import os
import shutil
from pathlib import Path
from typing import Optional, List
import numpy as np
import pandas as pd
from dflow.python import OP, OPIO, Artifact, OPIOSign



def get_low_e_isomer(info: pd.DataFrame, mode: str, rank: int=None, value: float=None):
    name_list = info['name']
    if mode == 'None':
        for a_name in name_list:
            yield a_name
    elif mode == 'rank':
        for a_name in name_list[:rank]:
            yield a_name
    else:
        from ase.units import Hartree, eV
        e_arr = np.array(info['e']) * Hartree / eV
        e_arr = e_arr - min(e_arr)
        uncutted_e = []
        for a_e in e_arr:
            if a_e > value:
                break
            uncutted_e.append(a_e)
        if mode == 'value':
            for idx in range(len(uncutted_e)):
                yield name_list[idx]
        else:
            if mode == 'value_and_rank' or mode == 'rank_and_value':
                for idx in range(min(len(uncutted_e), rank)):
                    yield name_list[idx]
            if mode == 'value_or_rank' or mode == 'rank_or_value':
                for idx in range(max(len(uncutted_e), rank)):
                    yield name_list[idx]


class simpleGen(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'in_xyz': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_gjf': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        from ase.io import read
        from ase.io.gaussian import write_gaussian_in
        atoms = read(op_in['in_xyz'])
        with open('gauss_in.gjf', 'w') as f:
            write_gaussian_in(fd=f,
                              atoms=atoms,
                              properties=[' '],
                              method="",
                              basis=" opt b3lyp/6-311G(d, p) empiricaldispersion=GD3",
                              chk='gauss_in.chk',
                              nprocshared='48',
                              mem='10GB',
                              )
        op_out = OPIO({
            "out_gjf": Path('gauss_in.gjf'),
        })
        return op_out


class gaussInGen(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'init': Artifact(Path),
            'info': Artifact(Path),
            'in_para': dict,
            'cutoff_para': dict,
            'cmd_line': str,
            'charge': int,
            'multi': int
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_raw': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        from ase.io import read
        from ase.io.gaussian import write_gaussian_in

        # 'cpu_per_task': int,
        # 'mem_per_task': str,
        # 'cmd_line': str,
        # 'cutoff_mode': str,
        # 'cutoff_rank': Optional[int],
        # 'cutoff_value': Optional[float],
        # 'charge': Optional[int],
        # 'multi': Optional[int]

        in_para = op_in['in_para']
        cwd_ = os.getcwd()
        os.makedirs('raw', exist_ok=True)
        info = pd.read_pickle(op_in['info'])
        abs_in_dir = os.path.abspath(op_in['init'])
        os.chdir('raw')
        for a_name in get_low_e_isomer(info=info, mode=op_in['cutoff_para']['mode'], value=op_in['cutoff_para']['value'], rank=op_in['cutoff_para']['rank']):
            dst_gjf = a_name+'.gjf'
            dst_chk = a_name+'.chk'
            in_atoms = read(filename=os.path.join(abs_in_dir, a_name+'.xyz'))
            with open(dst_gjf, 'w') as f:
                write_gaussian_in(fd=f,
                                  atoms=in_atoms,
                                  properties=[' '],
                                  method='',
                                  basis=op_in['cmd_line'],
                                  nprocshared=str(in_para['cpu_per_worker']),
                                  mem=in_para['mem_per_worker'],
                                  mult=op_in['multi'],
                                  charge=op_in['charge'],
                                  chk=dst_chk
                                  )
        os.chdir(cwd_)
        op_out = OPIO({
            "out_raw": Path('raw'),
        })
        return op_out


class xtbInGen(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'init': Artifact(Path),
            'info': Artifact(Path),
            'cutoff_para': dict,
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_raw': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        cwd_ = os.getcwd()
        os.makedirs('raw', exist_ok=True)
        info = pd.read_pickle(op_in['info'])
        abs_in_dir = os.path.abspath(op_in['init'])
        os.chdir('raw')
        for a_name in get_low_e_isomer(info=info, mode=op_in['cutoff_para']['mode'], value=op_in['cutoff_para']['value'], rank=op_in['cutoff_para']['rank']):
            shutil.copy(src=os.path.join(abs_in_dir, a_name+'.xyz'), dst=a_name+'.xyz')
        os.chdir(cwd_)
        op_out = OPIO({
            "out_raw": Path('raw'),
        })
        return op_out

class abcInGen(OP):
    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'init': Artifact(Path),
            'public_inp': Artifact(Path),
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'out_raw': Artifact(Path),
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:

        public_inp_path = op_in['public_inp']
        os.makedirs('raw', exist_ok=True)
        for old_name in os.listdir(op_in['init']):
            new_name = os.path.splitext(old_name)[0] + '.inp'
            new_path = os.path.join('raw', new_name)
            with open(public_inp_path, 'r') as f_r, open(file=new_path, mode='w') as f_w:
                f_r_lines = f_r.readlines()
                for idx, a_line in enumerate(f_r_lines):
                    f_w.writelines(a_line)
                    if a_line.startswith('components'):
                        break
                key_line = f_r_lines[idx + 1]
                new_key_line = old_name + ' ' + key_line.split()[1] + '\n'
                f_w.writelines(new_key_line)
                for a_line in f_r_lines[idx + 2:]:
                    f_w.writelines(a_line)
        op_out = OPIO({
            "out_raw": Path('raw'),
        })
        return op_out












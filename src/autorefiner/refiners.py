from typing import List, Union, Dict

from autorefiner.build import simpleBuild, BuildWithAux
from autorefiner.build import simpleParaSlice
from autorefiner.calculators import batchExe
from autorefiner.inputGen import gaussInGen, xtbInGen, abcInGen
from autorefiner.parser import gaussParser, xtbParser, abcParser
from dflow import Step, Steps, Inputs, InputArtifact, InputParameter, OutputArtifact, Outputs, argo_range
from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.python import PythonOPTemplate, Slices


class Refiner(Steps):
    def __init__(self, name: str = None,
                 inputs: Inputs = None,
                 outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None,
                 memoize_key: str = None,
                 annotations: Dict[str, str] = None,
                 cutoff_para: dict = None):
        super(Refiner, self).__init__(name=name, inputs=inputs, outputs=outputs,
                                      memoize_key=memoize_key, annotations=annotations, steps=steps)
        self.inputs.artifacts['init'] = InputArtifact()
        self.inputs.artifacts['info'] = InputArtifact(optional=True)

        self.outputs.artifacts['out_cooked'] = OutputArtifact()
        self.outputs.artifacts['info'] = OutputArtifact()
        if cutoff_para == None:
            self.cutoff_para = {'mode': 'None'}
        else:
            self.cutoff_para = cutoff_para
        cutoff_mode = self.cutoff_para['mode']
        if 'rank' in cutoff_mode:
            assert cutoff_para['rank'] != None, f'Please input a cutoff rank, since the cutoff mode is {cutoff_mode}.'
        else:
            self.cutoff_para.update({'rank': None})
        if 'value' in cutoff_mode:
            assert cutoff_para['value'] != None, f'Please input a cutoff value, since the cutoff mode is {cutoff_mode}.'
        else:
            self.cutoff_para.update({'value': None})

class Gau_Refiner(Refiner):
    def __init__(self, public_in_para: dict, image: str, dispatcher_executor: DispatcherExecutor,
                 cutoff_para: dict = None, has_chk_input: bool = False, keep_chk: bool = False,
                 has_slice: bool = False, cmd_line_list: list=None, prefix_list: list=None,

                 name: str = None,
                 inputs: Inputs = None,
                 outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None,
                 memoize_key: str = None,
                 annotations: Dict[str, str] = None):
        super(Gau_Refiner, self).__init__(cutoff_para=cutoff_para, name=name, inputs=inputs, outputs=outputs,
                                          memoize_key=memoize_key, annotations=annotations, steps=steps)

        # Parameters could be sliced, currently only support cmd_line.
        self.inputs.parameters['charge'] = InputParameter()
        self.inputs.parameters['multi'] = InputParameter()
        self.inputs.parameters['prefix'] = InputParameter()
        self.prefix = self.inputs.parameters['prefix']
        # To do: better way to perform slice, to support charge and multiplicity
        if has_slice:
            step_slice = Step(name='slice', template=PythonOPTemplate(simpleParaSlice, image=image),
                              parameters={
                                  'prefix': self.inputs.parameters['prefix'],
                                  'cmd_line_list': cmd_line_list,
                                  'prefix_list': prefix_list,
                              },
                              key=f"{self.prefix}-slice",)
            self.add(step_slice)
            step_inputGen = Step(
                name="inputGen",
                template=PythonOPTemplate(gaussInGen, image=image),
                artifacts={'init': self.inputs.artifacts['init'], 'info': self.inputs.artifacts['info']},
                parameters={'in_para': public_in_para,
                            'cutoff_para': self.cutoff_para,
                            'cmd_line': step_slice.outputs.parameters['cmd_line'],
                            'charge': self.inputs.parameters['charge'],
                            'multi': self.inputs.parameters['multi']
                            },
                key=f"{self.prefix}-inputGen",
            )
        else:
            self.inputs.parameters['cmd_line'] = InputParameter()
            step_inputGen = Step(
                name="inputGen",
                template=PythonOPTemplate(gaussInGen, image=image),
                artifacts={'init': self.inputs.artifacts['init'], 'info': self.inputs.artifacts['info']},
                parameters={'in_para': public_in_para,
                            'cutoff_para': self.cutoff_para,
                            'cmd_line': self.inputs.parameters['cmd_line'],
                            'charge': self.inputs.parameters['charge'],
                            'multi': self.inputs.parameters['multi']
                            },
                key=f"{self.prefix}-inputGen",
            )


        self.add(step_inputGen)

        if has_chk_input:
            self.inputs.artifacts['in_chk'] = InputArtifact()
            step_build = Step(
                name="build",
                template=PythonOPTemplate(BuildWithAux, image=image),
                artifacts={"in_workbase": step_inputGen.outputs.artifacts['out_raw'],
                           'in_aux': self.inputs.artifacts['in_chk']},
                parameters={'is_private': True},
                key=f"{self.prefix}-build",
            )
        else:
            step_build = Step(
                name="build",
                template=PythonOPTemplate(simpleBuild, image=image),
                artifacts={"in_workbase": step_inputGen.outputs.artifacts['out_raw']},
                key=f"{self.prefix}-build",
            )
        self.add(step_build)


        step_exe = Step(
            name='exe',
            template=PythonOPTemplate(batchExe, image=image),
            artifacts={'in_cooking': step_build.outputs.artifacts['out_workbase']},
            parameters={'in_para': public_in_para},
            executor=dispatcher_executor,
            key=f'{self.prefix}-exe',
        )
        self.add(step_exe)

        if keep_chk:
            self.outputs.artifacts['cooked_chk'] = OutputArtifact()
            step_parse = Step(
                name='parse',
                template=PythonOPTemplate(gaussParser, image=image),
                artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
                parameters={'in_para': public_in_para, 'prefix': self.prefix, 'keep_chk': True},
                key=f'{self.prefix}-parse',
            )
            self.add(step_parse)
            self.outputs.artifacts['cooked_chk']._from = step_parse.outputs.artifacts['cooked_chk']
        else:
            step_parse = Step(
                name='parse',
                template=PythonOPTemplate(gaussParser, image=image),
                artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
                parameters={'in_para': public_in_para, 'prefix': self.prefix, 'keep_chk': False},
                key=f'{self.prefix}-parse',
            )
            self.add(step_parse)
        self.outputs.artifacts['info']._from = step_parse.outputs.artifacts['info']
        self.outputs.artifacts['out_cooked']._from = step_parse.outputs.artifacts['out_cooked']

class xTB_Refiner(Refiner):
    def __init__(self,
                 public_in_para: dict,
                 image: str,
                 dispatcher_executor: DispatcherExecutor,
                 cutoff_para: dict = None,

                 name: str = None, inputs: Inputs = None, outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None, memoize_key: str = None,
                 annotations: Dict[str, str] = None):
        super(xTB_Refiner, self).__init__(cutoff_para=cutoff_para, name=name, inputs=inputs, outputs=outputs,
                                          memoize_key=memoize_key, annotations=annotations, steps=steps)

        self.prefix = self.name
        step_inputGen = Step(
            name="inputGen",
            template=PythonOPTemplate(xtbInGen, image=image),
            artifacts={'init': self.inputs.artifacts['init'], 'info': self.inputs.artifacts['info']},
            parameters={'cutoff_para': self.cutoff_para},
            key=f"{self.prefix}-inputGen",
        )
        self.add(step_inputGen)


        step_build = Step(
            name="build",
            template=PythonOPTemplate(simpleBuild, image=image),
            artifacts={"in_workbase": step_inputGen.outputs.artifacts['out_raw']},
            key=f"{self.prefix}-build",
        )

        self.add(step_build)

        step_exe = Step(
            name='exe',
            template=PythonOPTemplate(batchExe, image=image),
            artifacts={'in_cooking': step_build.outputs.artifacts['out_workbase']},
            parameters={'in_para': public_in_para},
            executor=dispatcher_executor,
            key=f'{self.prefix}-exe',
        )
        self.add(step_exe)

        step_parse = Step(
            name='parse',
            template=PythonOPTemplate(xtbParser, image=image),
            artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
            key=f'{self.prefix}-parse',
        )
        self.add(step_parse)

        self.outputs.artifacts['info']._from = step_parse.outputs.artifacts['info']
        self.outputs.artifacts['out_cooked']._from = step_parse.outputs.artifacts['out_cooked']

class ABC_Refiner(Refiner):
    def __init__(self,
                 public_in_para: dict,
                 image: str,
                 dispatcher_executor: DispatcherExecutor,
                 local_minima_folder: str,

                 name: str = None, inputs: Inputs = None, outputs: Outputs = None,
                 steps: List[Union[Step, List[Step]]] = None, memoize_key: str = None,
                 annotations: Dict[str, str] = None):
        super(ABC_Refiner, self).__init__(name=name, inputs=inputs, outputs=outputs,
                                          memoize_key=memoize_key, annotations=annotations, steps=steps)

        self.inputs.artifacts['public_inp'] = InputArtifact()
        self.prefix = self.name
        step_inputGen = Step(
            name="inputGen",
            template=PythonOPTemplate(op_class=abcInGen, image=image),
            artifacts={'init': self.inputs.artifacts['init'], 'public_inp': self.inputs.artifacts['public_inp']},
            key=f'{self.prefix}-inputGen'
        )
        self.add(step_inputGen)

        step_build = Step(
            name="build",
            template=PythonOPTemplate(BuildWithAux, image=image),
            artifacts={"in_workbase": step_inputGen.outputs.artifacts['out_raw'],
                       'in_aux': self.inputs.artifacts['init']},
            parameters={'is_private': True},
            key=f"{self.prefix}-build",
        )

        self.add(step_build)

        step_exe = Step(
            name='exe',
            template=PythonOPTemplate(batchExe, image=image),
            artifacts={'in_cooking': step_build.outputs.artifacts['out_workbase']},
            parameters={'in_para': public_in_para},
            executor=dispatcher_executor,
            key=f'{self.prefix}-exe',
        )
        self.add(step_exe)

        step_parse = Step(
            name='parse',
            template=PythonOPTemplate(abcParser, image=image),
            artifacts={'in_cooked': step_exe.outputs.artifacts['out_cooking']},
            parameters={'local_minima_folder': local_minima_folder, 'prefix': self.prefix},
            key=f'{self.prefix}-parse',
        )
        self.add(step_parse)

        self.outputs.artifacts['info']._from = step_parse.outputs.artifacts['info']
        self.outputs.artifacts['out_cooked']._from = step_parse.outputs.artifacts['out_cooked']



# -*- coding: utf-8 -*-
"""
The base restart workchain for UppASD includes three main steps:
1. Parse the input dict into the asd_calculation module.
2. Run the asd_calculation.
3. Check if the calculation is finished. If not, restart the calculation, add walltime, and then restart.
4. If the calculation is finished, return the results.

Noticed problems (ToDo list):
1. When the code automatically restarts from the restart file, it will lose the previous trajectory and only the last part of the trajectory will be saved.
** This problem is mostly due to the trajectory file being super large and currently not suitable for parsing several times.
!! The current solution is to ensure that enough walltime is given to the calculation that needs the trajectory.
Note that the PK of the workflow can be obtained by the following command:

baserestartworkflow = submit(UppASD_baseworkflow, **input_uppasd)
pk = baserestartworkflow.pk

The pk can be used for making groups of calculations for data query and analysis.

Dr. Qichen Xu, Uppsala University & KTH, Sweden
"""
from aiida import orm
from aiida.engine import (
    while_,
    BaseRestartWorkChain,
    process_handler,
    ProcessHandlerReport,
)
from aiida.plugins import CalculationFactory

# get calculations
ASDCalculation = CalculationFactory("asd_calculations")


class UppASD_Baseworkflow(BaseRestartWorkChain):
    # base restart workflow
    _process_class = ASDCalculation

    @classmethod
    def define(cls, spec):
        """Specify inputs and outputs."""
        super().define(spec)
        spec.expose_inputs(cls._process_class, exclude=["metadata"])
        # the workflow takes a dict as input, which contains everything needed for lunching the ASDCalculation
        # the following is the checking for required inputs:
        spec.input(
            "code",
            valid_type=orm.Code,
            required=True,
        )
        spec.input(
            "mpirun",
            valid_type=orm.Bool,
            required=True,
        )
        spec.input(
            "parser_name",
            valid_type=orm.Str,
            required=True,
        )
        spec.input(
            "input_dict",
            valid_type=orm.Dict,
            required=True,
        )
        spec.input(
            "retrieve_and_parse_name_list",
            valid_type=orm.List,
            required=True,
        )
        spec.input(
            "num_mpiprocs_per_machine",
            valid_type=orm.Int,
            help="The resource in cluster to use",
            required=True,
        )
        spec.input(
            "num_machines",
            valid_type=orm.Int,
            help="The resource in cluster to use",
            required=True,
        )
        # parameters for the auto restart those calculations hit the walltime
        spec.input(
            "init_walltime",
            valid_type=orm.Int,
            required=True,
        )
        spec.input(
            "calculation_repeat_num",
            valid_type=orm.Int,
            required=True,
        )
        spec.input(
            "walltime_increase",
            valid_type=orm.Int,
            required=True,
        )
        # Lables and descriptions for this sub-calculation, good for human reading.
        spec.input(
            "label",
            valid_type=orm.Str,
            required=True,
        )
        spec.input(
            "description",
            valid_type=orm.Str,
            required=True,
        )

        # Input tags for restart model
        # Nstep for the ASD calculation, S mode
        # mcNstep for the MC calculation, H and M mode
        spec.input(
            "autorestart_mode",
            valid_type=orm.Str,
            required=True,
        )
        # the outline of the workflow
        spec.outline(
            # step 1 : process the input data
            cls.setup,
            cls.inputs_process,
            # step 2 : run the ASD calculation and check walltime error, this part is done by AiiDA, and we refer to it by error handler
            while_(cls.should_run_process)(
                cls.run_process,
                cls.inspect_process,
            ),
            # step 3 : process the results, report if the calculation is not done.
            cls.results,
        )

        spec.expose_outputs(cls._process_class)
        # register the exit codes
        # spec.exit_code(451, "WallTimeError", message="Hit the max wall time")

    def inputs_process(self):
        self.report("Processing input data for ASDcalculation")
        # The wapper for the ASD calculation inputs:
        self.ctx.inputs = {
            "code": self.inputs.code,
            "input_dict": self.inputs.input_dict,
            "retrieve_and_parse_name_list": self.inputs.retrieve_and_parse_name_list,
            "metadata": {
                "options": {
                    "resources": {
                        "num_machines": self.inputs.num_machines.value,
                        "num_mpiprocs_per_machine": self.inputs.num_mpiprocs_per_machine.value,
                    },
                    "max_wallclock_seconds": self.inputs.init_walltime.value,
                    "parser_name": self.inputs.parser_name.value,
                    # withmpi should be True for all ASD calculations, I can not see any reason to run ASD calculation without mpi
                    "withmpi": self.inputs.mpirun.value,
                },
                "label": self.inputs.label.value,
                "description": self.inputs.description.value,
            },
        }

    def results(self):
        # get the last calculation node
        max_repeat = self.inputs.calculation_repeat_num.value
        node = self.ctx.children[self.ctx.iteration - 1]
        if not self.ctx.is_finished and self.ctx.iteration >= max_repeat:
            # report the last attempt
            self.report(
                f"Reached the maximum number of repeat for walltime error {max_repeat}: "
                f"The last attempt is:  {self.ctx.process_name}<{node.pk}>"
            )
            return self.exit_codes.ERROR_MAXIMUM_ITERATIONS_EXCEEDED

        self.report(
            f"The baseworkflow for label: {self.inputs.label}  and with description: {self.inputs.description} is completed after {self.ctx.iteration} iterations"
        )

        exposed_outputs = self.exposed_outputs(node, self._process_class)
        self.out_many(exposed_outputs)
        return None

    # Error handler
    @process_handler(
        priority=500,
        exit_codes=[
            _process_class.exit_codes.WallTimeError,
        ],
    )

    # Define the walltime handler
    def walltime_error_handler(self, node):
        self.report("WallTimeError happened")
        # Get previous calculation node
        node = self.ctx.children[self.ctx.iteration - 1]
        # Get output_array using node pk
        previous_cal_pk = node.pk

        qb = orm.QueryBuilder()
        qb.append(orm.CalcJobNode, filters={"id": str(previous_cal_pk)}, tag="cal_node")
        qb.append(orm.ArrayData, with_incoming="cal_node", tag="arrays")
        all_array = qb.all()
        for array in all_array:
            for name in array[0].get_arraynames():
                if name == "restart":
                    restart_array = array[0].get_array(name)
                    break
        restart_list = []
        head_of_restartfile = [
            """################################################################################
# File type: AiiDA-UppASD2 auto-restart file
# Simulation type: AiiDA-UppASD2 workflow
# Number of atoms:   According to main workflow
# Number of ensembles:        According to main workflow
################################################################################
   #iterens   iatom           |Mom|             M_x             M_y             M_z"""
        ]

        restart_list.append(head_of_restartfile)
        for i in restart_array:
            i_list = i.tolist()
            i_list[0] = int(i_list[0])
            i_list[1] = int(i_list[1])
            i_list[2] = int(i_list[2])
            restart_list.append(i_list)

        # set Initmag 4 and write restartfile
        # since all stored nodes are immutable, we need to create a new dict and store it in the new input_dict
        new_input_dict = self.ctx.inputs["input_dict"].get_dict()
        new_input_dict["inpsd"]["Initmag"] = "4"
        new_input_dict["inpsd"]["restartfile"] = [
            orm.Str(f"AiiDA_UppASD2_Walltime_restart_{self.ctx.iteration - 1}")
        ]
        new_input_dict[f"AiiDA_UppASD2_Walltime_restart_{self.ctx.iteration - 1}"] = (
            restart_list
        )
        restart_steps = int(
            new_input_dict["inpsd"][self.inputs.autorestart_mode.value][0]
        ) - int(restart_array[0].tolist()[0])
        new_input_dict["inpsd"][self.inputs.autorestart_mode.value] = [
            str(restart_steps)
        ]
        self.ctx.inputs["input_dict"] = orm.Dict(dict=new_input_dict)

        self.ctx.inputs["metadata"]["options"][
            "max_wallclock_seconds"
        ] += self.inputs.walltime_increase.value
        self.report(
            f"Restarting the ASD calculation with walltime {self.ctx.inputs['metadata']['options']['max_wallclock_seconds']}"
        )
        return ProcessHandlerReport(do_break=False)

"""
This workflow is a generic loop workflow that can be used to run any workflow for looping UppASD calculations, the output is a orm.Dict 
object that contains the pk of done sub-workflows and the descriptive tags for those sub-workflows.

One typical example is the Skyrmion Temp-Field phase diagram workflow, which is a workflow for running UppASD 
calculations (or mostly the simulated annealing calculations) for a series of temperature and field values.

The workflow is based on the BaseRestartWorkChain, which is a sub-workflow that can be used to run a single 
calculation and automatically restart the calculation if it is not finished (from the restart file).

The only special input to this workflow compared to the BaseRestartWorkChain is an orm.Dict object that contains 
the parameters for looping the calculation. For example:

"loop_dict_input": orm.Dict(
        dict={
            "temp": [["10"], ["50"], ["100"]],
            "hfield": [
                ["0", "0", "10"],
                ["0", "0", "50"],
                ["0", "0", "100"],
            ],
            "ncell": [["50", "50", "1"], ["120", "120", "1"]],
        }
    ),

! Important: The value of each list should be a string, even if it is a number. See the example above.

The output of the workflow is an orm.Dict where the keys are the values of the input dict above and the values are the pk of 
the BaseRestartWorkChain. Users can use those pk for further analysis or data query.

Tips:
1. We highly recommend setting a very high walltime or a high number of auto restart times for the BaseRestartWorkChain
in case the loop workflow breaks.
!ToDo: Add features to automatically restart the loop workflow if it breaks. 

Warning: 
1. Since we detect the input variable by name, please make sure not to use the same name for representation of tag and file.
For example, using the momfile ./momfile in the inpsd and using momfile as an input file is not allowed.

2. This workflow is designed for making changes to existing calculations, no new flags should be introduced via this 
workflow. For example, no new flag should be put in loop_dict_input for a new flag in inpsd_dict.

Any questions, contact: qichenx@kth.se

Dr. Qichen Xu, Uppsala University & KTH, Sweden

"""

import re
from aiida_uppasd2.UppASD_BaseWorkflow import UppASD_Baseworkflow
from aiida import orm
from aiida.engine import (
    ToContext,
    WorkChain,
)


class GenericLoopWorkflow(WorkChain):

    @classmethod
    def define(cls, spec):
        """Specify inputs and outputs."""
        super().define(spec)

        # Get all the inputs from the BaseRestartWorkChain
        spec.expose_inputs(UppASD_Baseworkflow)

        # Define loop_dict
        spec.input("loop_dict_input", valid_type=orm.Dict, required=True)

        # Define the output of the workflow
        spec.output("loop_dict_output_pk", valid_type=orm.Dict, required=False)

        # Define a error code for when subworkflow fails
        spec.exit_code(
            400, "ERROR_SUB_LOOP_WORKFLOW_FAILED", message="Subworkflow in loop failed"
        )

        spec.outline(
            cls.loops,
            cls.inspect_and_summarize,
        )

    def generate_loops(
        self,
        loop_dict_input,
        func,
        keys,
        keys_for_fuction,
        sub_workflow_dict,  # An empty dict for store all the sub workflows results
        loop_dict_combinations,  # This is a list for store all tags for next step use
        current_combination=None,
    ):
        if current_combination is None:
            current_combination = []

        if not keys:
            # keys_for_fuction  initial value is equal to keys, used for change dict keys for
            # the auto_restart_workflow
            func(
                current_combination,
                keys_for_fuction,
                sub_workflow_dict,
                loop_dict_combinations,
            )
            return

        current_key = keys[0]
        remaining_keys = keys[1:]

        for value in loop_dict_input[current_key]:
            self.generate_loops(
                loop_dict_input,
                func,
                remaining_keys,
                keys_for_fuction,
                sub_workflow_dict,
                loop_dict_combinations,
                current_combination + [value],
            )

    def submit_base_restart_workflow(
        self,
        current_combination,
        keys_for_fuction,
        sub_workflow_dict,
        loop_dict_combinations,
    ):
        workflow_input_dict = {
            "code": self.inputs.code,
            "mpirun": self.inputs.mpirun,
            "input_dict": self.inputs.input_dict.get_dict(),
            "retrieve_and_parse_name_list": self.inputs.retrieve_and_parse_name_list,
            "label": self.inputs.label,
            "description": self.inputs.description,
            "parser_name": self.inputs.parser_name,
            "num_mpiprocs_per_machine": self.inputs.num_mpiprocs_per_machine,
            "num_machines": self.inputs.num_machines,
            "init_walltime": self.inputs.init_walltime,
            "calculation_repeat_num": self.inputs.calculation_repeat_num,
            "walltime_increase": self.inputs.walltime_increase,
            "autorestart_mode": self.inputs.autorestart_mode,
        }

        sub_workflow_tag = ""
        for i in range(len(keys_for_fuction)):
            sub_workflow_tag = (
                sub_workflow_tag
                + str(keys_for_fuction[i])
                + "_"
                + str(current_combination[i])
                + "_"
            )
            # Check if this key is in the which level of the input_dict
            # note that if it is not in the first level, it will be in inpsd dict
            if keys_for_fuction[i] in workflow_input_dict["input_dict"].keys():
                workflow_input_dict["input_dict"][keys_for_fuction[i]] = (
                    current_combination[i]
                )

            else:
                workflow_input_dict["input_dict"]["inpsd"][keys_for_fuction[i]] = (
                    current_combination[i]
                )

        # Repalce all symbols that are not allowed in the tag to '_'
        sub_workflow_tag = re.sub(r"[^a-zA-Z0-9_]", "_", sub_workflow_tag)
        loop_dict_combinations.append(sub_workflow_tag)
        future = self.submit(UppASD_Baseworkflow, **workflow_input_dict)
        sub_workflow_dict[sub_workflow_tag] = future

    def loops(self):
        # Generate the inputs for the BaseRestartWorkChain using the loop_dict_input
        # Step 1 : get the loop_dict_input and count the number of loops
        loop_dict_input = self.inputs.loop_dict_input.get_dict()
        loop_dict_input_keys = list(loop_dict_input.keys())
        loop_dict_combinations = []  # store all the combinations tags for next step use
        sub_workflow_dict = {}  # a empty dict for store all the sub workflows results
        # Step 2: generate all the combinations for the loops and submit and submit the sub workflows.
        self.generate_loops(
            loop_dict_input,
            self.submit_base_restart_workflow,
            loop_dict_input_keys,
            loop_dict_input_keys,
            sub_workflow_dict,
            loop_dict_combinations,
        )
        # Step 3: store all tags for next step use into ctx
        self.ctx.loop_dict_combinations = loop_dict_combinations
        # Step 4: return all calculated sub workflows to context
        return ToContext(**sub_workflow_dict)

    def inspect_and_summarize(self):
        # We need the pk of the sub workflows
        loop_dict_output_pk = {}
        # Check if all the sub workflows are finished okay
        for tag in self.ctx.loop_dict_combinations:
            if not self.ctx[tag].is_finished_ok:
                return self.exit_codes.ERROR_SUB_LOOP_WORKFLOW_FAILED
        all_tags = self.ctx.loop_dict_combinations
        for tag in all_tags:
            loop_dict_output_pk[tag] = self.ctx[tag].pk
        self.out("loop_dict_output_pk", orm.Dict(dict=loop_dict_output_pk).store())

"""
Main module for UppASD calculations.
Author: Qichen Xu, PhD, Uppsala University, Sweden
Date: 2024-07-11
"""

# Importing modules.
# Please keep in mind to remove unnecessary modules in future versions.
from aiida import orm
from aiida.common import datastructures
from aiida.engine import CalcJob


# Main class for UppASD calculations.
class UppasdBaseCalculation(CalcJob):
    @classmethod
    def define(cls, spec):
        super(UppasdBaseCalculation, cls).define(spec)
        # input file sections :
        spec.input(
            "input_dict",
            valid_type=orm.Dict,
            required=True,
            help="dict from loading the uppasd_aiida2_input.pkl file",
        )
        # if no retrieve_list_name included, nothing will be retrieved from the remote folder, only parser the total energy file back
        spec.input(
            "retrieve_and_parse_name_list",
            valid_type=orm.List,
            required=True,
            help="list of files to parse and retrieve from the remote folder",
        )
        # output sections:
        spec.output(
            "output_array",
            valid_type=orm.ArrayData,
            help="the output arrays of a single UppASD calculation, it includes all request files (parsed into np arrays) in the retrieve_list_name",
            required=True,
        )

        spec.exit_code(451, "WallTimeError", message="Hit the max wall time")

    def prepare_for_submission(self, folder):

        calcinfo = datastructures.CalcInfo()

        # let's firstly dispatch the input dict into several input file in the sandbox
        uppasd_aiida2_input_dict = self.inputs.input_dict.get_dict()
        # in the input dict, we assume the keys represent the file name and the value is file content.
        input_file_name_list = uppasd_aiida2_input_dict.keys()
        for file_name in input_file_name_list:
            if file_name == "inpsd":
                with folder.open((file_name + ".dat"), "a+") as f:
                    for i in uppasd_aiida2_input_dict["inpsd"].keys():
                        value = " ".join(map(str, uppasd_aiida2_input_dict["inpsd"][i]))
                        f.write("{} {}\n".format(i, value.replace("\\n", "\n")))

            else:
                if "qfile" in file_name:  # qfile is special, it has a header line
                    data_list = uppasd_aiida2_input_dict[file_name]
                    with folder.open(file_name, "w") as file:
                        file.write(" {} \n".format(len(data_list)))
                        for sublist in data_list:
                            line = " ".join(map(str, sublist))
                            file.write(line + "\n")
                else:
                    data_list = uppasd_aiida2_input_dict[file_name]
                    with folder.open(file_name, "w") as file:
                        for sublist in data_list:
                            line = " ".join(map(str, sublist))
                            file.write(line + "\n")

        # calcinfo.local_copy_list = []
        codeinfo = datastructures.CodeInfo()
        # codeinfo.cmdline_params = []
        codeinfo.code_uuid = self.inputs.code.uuid
        calcinfo.codes_info = [codeinfo]
        calcinfo.retrieve_list = self.inputs.retrieve_and_parse_name_list.get_list()
        return calcinfo

# -*- coding: utf-8 -*-
"""
Parser for UppASD
"""
import json
import numpy as np
import pandas as pd
from aiida import orm
from aiida.engine import ExitCode
from aiida.parsers.parser import Parser
from aiida.plugins import CalculationFactory
from aiida.common.exceptions import NotExistent
from aiida.orm import (
    Code,
    SinglefileData,
    BandsData,
    Int,
    Float,
    Str,
    Bool,
    List,
    Dict,
    ArrayData,
    XyData,
    SinglefileData,
    FolderData,
    RemoteData,
)

ASDCalculation = CalculationFactory("asd_calculations")


class UppASD_Parsers(Parser):
    # Some exceptions: aniso.out,
    def aniso_struct_out_parser(sekf, input_file):
        f = open(input_file)
        all_file = f.readlines()[1:]
        index = 0
        for i in range(int((len(all_file) / 4))):
            if i == 0:
                atom_number = np.array([i])
                easy_axis = np.array(np.float_(all_file[index + 2].strip("\n").split()))
                aniso_K = np.array(np.float_(all_file[index + 3].strip("\n").split()))

            else:
                atom_number = np.vstack([atom_number, np.array([i])])
                easy_axis = np.vstack(
                    [
                        easy_axis,
                        np.array(np.float_(all_file[index + 2].strip("\n").split())),
                    ]
                )
                aniso_K = np.vstack(
                    [
                        aniso_K,
                        np.array(np.float_(all_file[index + 3].strip("\n").split())),
                    ]
                )

            index = index + 4
        aniso_full = np.hstack([atom_number, easy_axis, aniso_K])
        return aniso_full

    # General parser for tensor output
    def get_skiplines(
        self, input_file
    ):  # pylint: disable=too-many-locals, too-many-statements, too-many-branches
        # Read line by one to check which line is not the comment line, the reason why we don't use pd.read_csv(file_name, comment='#') is because we want to use sep="\s+".
        skipline = 0
        while True:
            line = input_file.readline()
            if not line.lstrip().startswith(
                b"#"
            ):  # we here use thebyte mode so a string should be b"#"
                break
            skipline = skipline + 1
        return skipline

    def general_parse(
        self, input_file, skipline
    ):  # pylint: disable=too-many-locals, too-many-statements, too-many-branches
        # Read line by one to check which line is not the comment line, the reason why we don't use pd.read_csv(file_name, comment='#') is because we want to use sep="\s+".
        output_tensor = pd.read_csv(
            input_file, sep="\s+", header=None, skiprows=skipline
        ).to_numpy()
        return output_tensor

    def parse(self, **kwargs):
        output_folder = self.retrieved

        retrived_file_name_list = output_folder.list_object_names()

        # Check if all requested files are present
        files_requested = self.node.inputs.retrieve_and_parse_name_list.get_list()

        # if not set(files_requested) <= set(retrived_file_name_list):
        #     self.logger.error(
        #         f"Found files '{retrived_file_name_list} in remote folder', but request: '{files_requested}', pls check your UppASD input"
        #     )
        #     return self.exit_codes.ERROR_MISSING_OUTPUT_FILES

        # Walltime check
        output_arrays = ArrayData()
        for filename in files_requested:
            # parser special files:
            if "aniso" in filename:
                aniso_filename = filename
                if filename[-1] == "*":
                    filename = (
                        filename[:-1]
                        + "."
                        + self.node.inputs.input_dict["inpsd"]["simid"][0]
                        + ".out"
                    )
                if filename in retrived_file_name_list:
                    with output_folder.open(aniso_filename, "rb") as f:
                        aniso = self.aniso_struct_out_parser(f)
                        output_arrays.set_array(
                            filename.split(".")[0], aniso
                        )  # .split('.')[0] for name like 'aniso.xxx.out' to 'aniso'
                # parser general files:
            else:
                if filename[-1] == "*":
                    filename = (
                        filename[:-1]
                        + "."
                        + self.node.inputs.input_dict["inpsd"]["simid"][0]
                        + ".out"
                    )
                if filename in retrived_file_name_list:
                    with output_folder.open(filename, "rb") as f:
                        skipline = self.get_skiplines(f)
                    with output_folder.open(filename, "rb") as f:
                        output = self.general_parse(f, skipline)
                    output_arrays.set_array(filename.split(".")[0], output)
        self.out("output_array", output_arrays)
        # after return current result we can check if the walltime is reached
        with output_folder.open("_scheduler-stdout.txt", "rb") as handler:
            log = str(handler.read())
            if "Simulation finished" in log:
                return ExitCode(0)
            else:
                return ASDCalculation.exit_codes.WallTimeError

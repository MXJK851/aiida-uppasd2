# -*- coding: utf-8 -*-
##!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 15:15:05 2021

@author: qichen
"""
from aiida import orm, load_profile
from aiida.engine import submit
import pickle
from aiida_uppasd.workflows.generic_workflow import UppasdGenericLoopWorkflow

load_profile()

code = orm.load_code("lumidebug_uppasd@lumidebug")
with open("./uppasd_aiida2_input.pkl", "rb") as f:
    inpsd_dict_load = pickle.load(f)
inpsd_dict_load["inpsd"]["Nstep"] = ["50000"]
input_dict = orm.Dict(dict=inpsd_dict_load)

workflow_input_dict = {
    "code": code,
    "mpirun": orm.Bool(False),
    "input_dict": input_dict,
    "retrieve_and_parse_name_list": orm.List(["totenergy*", "restart*", "sknumber*"]),
    "label": orm.Str("test1 label"),
    "description": orm.Str("test1 description"),
    "parser_name": orm.Str("asd_parsers"),
    "num_mpiprocs_per_machine": orm.Int(1),
    "num_machines": orm.Int(1),
    "init_walltime": orm.Int(100),
    "calculation_repeat_num": orm.Int(5),
    "walltime_increase": orm.Int(100),
    "autorestart_mode": orm.Str("Nstep"),
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
}

process = submit(UppasdGenericLoopWorkflow.get_builder(), **workflow_input_dict)

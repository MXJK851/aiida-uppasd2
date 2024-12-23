# -*- coding: utf-8 -*-
##!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 15:15:05 2021

@author: qichen
"""
from aiida import orm, load_profile
from aiida.plugins import CalculationFactory
from aiida.engine import submit
import pickle

load_profile()
code = orm.load_code("local_code_uppasd@localhost")
aiida_uppasd = CalculationFactory("uppasd.base")
builder = aiida_uppasd.get_builder()

with open("./uppasd_aiida2_input.pkl", "rb") as f:
    inpsd_dict_load = pickle.load(f)


input_dict = orm.Dict(dict=inpsd_dict_load)
builder.code = code
builder.input_dict = input_dict
builder.retrieve_and_parse_name_list = orm.List(["totenergy*", "restart*"])
builder.metadata.options.resources = {"num_machines": 1, "num_mpiprocs_per_machine": 8}
builder.metadata.options.max_wallclock_seconds = 55
builder.metadata.options.parser_name = "asd_parsers"
builder.metadata.label = "aiida_uppasd2_calculation"
builder.metadata.description = "Calculation Example for aiida-uppasd2"
job_node = submit(builder)
print(f"Job submitted, PK: {job_node.pk}")

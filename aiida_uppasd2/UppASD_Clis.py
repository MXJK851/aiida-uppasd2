import pandas as pd
import click
import pickle
import os
from aiida import orm


@click.group()
def asd():
    """Command line interface for aiida-uppasd2"""


@asd.command("uppasd_raw_input_parser")
@click.argument("inputs", nargs=-1)
def uppasd_raw_input_parser(inputs):
    """
    One example is: verdi data asd uppasd_raw_input_parser inpsd.dat momfile jij dmdata qfile posfile

    Note that we now can only translate clear version of inpsd.dat
    Our user need to remove commonts that without sign (a new line that start with #),i.e.,BC  P P P              Boundary conditions (0=vacuum,P=periodic)
    We also request one tage occupy only one line, e.g, cell       1.00000  0.00000   0.00000  0.00000   1.00000   0.00000  0.00000   0.00000   1.00000

    """
    uppasd_input_dict = {}
    for file_name in inputs:
        if file_name == "inpsd.dat":
            inpsd_dict = {}
            with open("inpsd.dat") as file:
                inpsd_file = file.readlines()

            for line_number in range(len(inpsd_file)):
                if (
                    inpsd_file[line_number][0] != " "
                    and inpsd_file[line_number][0] != "\n"
                    and inpsd_file[line_number][0] != "#"
                ):
                    inpsd_dict[inpsd_file[line_number].split()[0]] = inpsd_file[
                        line_number
                    ].split()[1:]
            uppasd_input_dict["inpsd"] = inpsd_dict

        else:
            with open(file_name) as file:
                file_cont = file.readlines()
                for i in range(len(file_cont)):
                    if file_cont[i].lstrip()[0] != "#":
                        skiplines = i
                        break
            df = pd.read_csv(file_name, sep="\s+", header=None, skiprows=skiplines)
            uppasd_input_dict[file_name] = [*map(list, zip(*map(df.get, df)))]
    with open("./uppasd_aiida2_input.pkl", "wb") as f:
        pickle.dump(uppasd_input_dict, f)


@asd.command("retrieve_restart_file")
@click.argument("pk", nargs=-1)
def retrieve_restart_file(pk):
    """
    One example is: verdi data asd retrieve_restart_file 2854

    Here 2854 is one PK for a baseworkflow or a calculation
    """
    head_of_restartfile = """################################################################################
# File type: AiiDA-UppASD2 cli retrived restart file
# Simulation type: AiiDA-UppASD2 workflow
# Number of atoms:   According to main workflow
# Number of ensembles:        According to main workflow
################################################################################
   #iterens   iatom           |Mom|             M_x             M_y             M_z
"""

    cal_node_pk = pk[0]
    qb = orm.QueryBuilder()

    # Identify the node type:
    node = orm.load_node(cal_node_pk)
    if node.node_type == "process.calculation.calcjob.CalcJobNode.":
        qb.append(orm.CalcJobNode, filters={"id": str(cal_node_pk)}, tag="cal_node")
    elif node.node_type == "process.workflow.workchain.WorkChainNode.":
        qb.append(orm.WorkChainNode, filters={"id": str(cal_node_pk)}, tag="cal_node")
    else:
        raise ValueError("The input node type is not supported")

    qb.append(orm.ArrayData, with_incoming="cal_node", tag="arrays")
    all_array = qb.all()
    array_name = all_array[0][0].get_arraynames()
    if "restart" in array_name:
        restart_moment = all_array[0][0].get_array("restart")
        restart_moment_pd = pd.DataFrame(restart_moment)
        # Set column 0 1 2 to int
        restart_moment_pd[0] = restart_moment_pd[0].astype(int)
        restart_moment_pd[1] = restart_moment_pd[1].astype(int)
        restart_moment_pd[2] = restart_moment_pd[2].astype(int)
        # check if the restart file is existed or not, if exist, remove it first
        try:
            os.remove("./restart.PK_{}.out".format(cal_node_pk))
        except:
            pass

        # Save_to_csv
        restart_moment_pd.to_csv(
            "./restart.PK_{}.out".format(cal_node_pk),
            index=False,
            header=False,
            sep=" ",
        )
        # Adding head of restartfile
        with open("./restart.PK_{}.out".format(cal_node_pk), "r+") as f:
            original = f.read()
            f.seek(0)
            f.write("{}".format(head_of_restartfile))
            f.write(original)
    else:
        raise ValueError("The restart array is not found in the given node")

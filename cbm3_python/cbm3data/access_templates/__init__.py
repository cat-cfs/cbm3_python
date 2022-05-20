# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os
import shutil


def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))


def make_dir(destination):
    if not os.path.exists(os.path.dirname(destination)):
        os.makedirs(os.path.dirname(destination))


def copy_mdb_template(destination):
    make_dir(destination)
    shutil.copy(os.path.join(get_script_dir(), "blank.mdb"), destination)


def copy_accdb_template(destination):
    make_dir(destination)
    shutil.copy(os.path.join(get_script_dir(), "blank.accdb"), destination)


def copy_rrdb_template(destination):
    make_dir(destination)
    shutil.copy(
        os.path.join(get_script_dir(), "RRDB_Template.accdb"), destination
    )


def copy_rollup_template(destination):
    make_dir(destination)
    shutil.copy(
        os.path.join(get_script_dir(), "rollup_template.accdb"), destination
    )

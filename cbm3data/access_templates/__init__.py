import os, shutil

def get_script_dir():
    return os.path.dirname(os.path.realpath(__file__))

def copy_mdb(destination):
    shutil.copy(os.path.join(get_script_dir(), "blank.mdb"), destination)

def copy_accdb(destination):
    shutil.copy(os.path.join(get_script_dir(), "blank.accdb"), destination)

def copy_rrdb_template(destination):
    shutil.copy(os.path.join(get_script_dir(), "RRDB_Template.accdb"), destination)

# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os, shutil
import win32com.client


def compact_and_repair(access_db_path):
    '''
    runs the compact & repair function on the database at the specified access_db_path
    mostly borrowed from: https://stackoverflow.com/questions/39083000/find-all-access-databases-and-compact-and-repair
    '''
    # LAUNCH ACCESS APP
    oApp = win32com.client.Dispatch("Access.Application")

    fn, ext = os.path.splitext(access_db_path)
    if ext.lower() not in [".mdb", ".accdb"]:
        raise ValueError("specified extension ({}) not supported for compact & repair"
                         .format(ext))
    bkfile = "{0}{1}{2}".format(fn, "_bk", ext)

    oApp.CompactRepair(access_db_path, bkfile, False)

    shutil.copyfile(bkfile, access_db_path)
    os.remove(bkfile)

    oApp = None

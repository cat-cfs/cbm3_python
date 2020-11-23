import os


def get_archive_index_path():
    return os.path.join(
        get_install_path(), "Admin", "DBs", "ArchiveIndex_Beta_Install.mdb")


def get_cbm_executable_dir():
    return os.path.join(
        get_install_path(), "Admin", "Executables")


def get_install_path():
    program_files_dir = os.path.join(
        "c:\\", "Program Files (x86)", "Operational-Scale CBM-CFS3")
    appdata_dir = os.path.join(
        os.getenv("LOCALAPPDATA"), "Programs",
        "Operational-Scale CBM-CFS3")

    use_program_files_dir = os.path.exists(program_files_dir) and \
        os.path.exists(os.path.join(program_files_dir, "Toolbox.exe"))
    use_appdata_dir = os.path.exists(appdata_dir) and \
        os.path.exists(os.path.join(appdata_dir, "Toolbox.exe"))
    if use_program_files_dir and appdata_dir:
        raise ValueError(
            "multiple toolbox installations detected: ",
            f"'{program_files_dir}', '{appdata_dir}'")
    elif use_appdata_dir:
        return appdata_dir
    elif use_program_files_dir:
        return program_files_dir
    else:
        raise ValueError(
            "cannot find installed toolbox at default directories")

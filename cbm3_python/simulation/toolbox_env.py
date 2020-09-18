import os
import shutil
import fnmatch
from os.path import isdir, join
import xml.etree.ElementTree as ET


# credit to:
# https://stackoverflow.com/questions/52071642/python-copying-the-files-with-include-pattern
def include_patterns(*patterns):
    """Factory function that can be used with copytree() ignore parameter.

    Arguments define a sequence of glob-style patterns
    that are used to specify what files to NOT ignore.
    Creates and returns a function that determines this for each directory
    in the file hierarchy rooted at the source directory when used with
    shutil.copytree().
    """
    def _ignore_patterns(path, names):
        keep = set(name for pattern in patterns
                   for name in fnmatch.filter(names, pattern))
        ignore = set(name for name in names
                     if name not in keep and not isdir(join(path, name)))
        return ignore
    return _ignore_patterns


def create_toolbox_env(toolbox_installation_path, toolbox_environment_path):
    """Clone the toolbox installation so that multiple CBM3 simulations can be
    run concurrently.

    Args:
        toolbox_installation_path (str): path to the installed
            Operational-Scale CBM-CFS3 toolbox.
        toolbox_environment_path (str): writeable path to a directory where
            the toolbox will be cloned.
    """

    included = include_patterns(
        "*.exe", "*.dll", "*.exe.config", "*.xml", "*.db",
        "Global.cbmproj", "CBMToolbox.xml"
    )
    shutil.copytree(
        src=toolbox_installation_path,
        dst=toolbox_environment_path,
        ignore=included)
    env_db_dir = os.path.join(toolbox_environment_path, "Admin", "DBs")
    os.rmdir(env_db_dir)
    shutil.copytree(
        src=os.path.join(toolbox_installation_path, "Admin", "DBs"),
        dst=env_db_dir)
    update_toolbox_env_paths(toolbox_environment_path)


def _update_toolbox_env_paths(xmlPath, toolbox_environment_path):
    tree = ET.parse(xmlPath)
    root = tree.getroot()

    dir_node_updates = [
        {"node": "ArchiveIndexDbPath", "subdir": os.path.join("Admin", "DBs")},
        {"node": "InputDbTemplatePath",
         "subdir": os.path.join("Admin", "DBs")},
        {"node": "SimulationSchedulerWorkingDirectory", "subdir": "Temp"}]
    path_node_updates = [
        {"node": "LoaderTemplateDbPath",
         "subdir": os.path.join("Admin", "DBs")},
        {"node": "RunResultsTemplateDbPath",
         "subdir": os.path.join("Admin", "DBs")},
        {"node": "BVERulesDbPath", "subdir": os.path.join("Admin", "DBs")},
        {"node": "PreMadeViewsNameAndPath", "subdir": "Views"}]

    for dir_node_updates in dir_node_updates:
        dir_node = root[0].find(dir_node_updates["node"])
        dir_node.set(
            "value", os.path.join(
                toolbox_environment_path, dir_node_updates["subdir"]) + "\\")
        # the trailing slash here is the required format in the toolbox
        # xml configs

    for path_node_update in path_node_updates:
        path_node = root[0].find(path_node_update["node"])
        path_node_file_name = os.path.basename(path_node.get("value"))
        path_node.set(
            "value", os.path.join(
                toolbox_environment_path,
                path_node_update["subdir"],
                path_node_file_name))

    tree.write(xmlPath)


def update_toolbox_env_paths(toolbox_environment_path):
    """Update the toolbox configuration to use the toolbox environment path.

    Args:
        toolbox_environment_path (str): path to the cloned toolbox environment

    Raises:
        ValueError: Raised if the specified path is relative, absolute paths
            are required.
    """
    if not os.path.isabs(toolbox_environment_path):
        raise ValueError(
            f"specified path is relative '{toolbox_environment_path}'")

    _update_toolbox_env_paths(
        os.path.join(toolbox_environment_path, "Global.cbmproj"),
        toolbox_environment_path)
    _update_toolbox_env_paths(
        os.path.join(toolbox_environment_path, "CBMToolbox.xml"),
        toolbox_environment_path)

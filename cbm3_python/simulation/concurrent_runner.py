import os
import shutil
from concurrent.futures import ProcessPoolExecutor
import tempfile

from cbm3_python.simulation import toolbox_env
from cbm3_python.simulation import projectsimulator
from cbm3_python.util import loghelper


class ConcurrentRunner:

    def __init__(self, toolbox_path):
        self.toolbox_path = toolbox_path

    def run_func(self, run_args):
        """Calls :py:func:`cbm3_python.simulation.projectsimulator.run`
        using the specified args. This function also sets up a toolbox
        environment for safely running CBM3 as multiple processes.

        Args:
            run_args (dict): arguments to
                :py:func:`cbm3_python.simulation.projectsimulator.run`
                in dictionary form.

        Raises:
            ValueError: raised if particular required arguments have been
                omitted from run args.

                The required arguments are:

                    * aidb_path
                    * cbm_exe_path
                    * results_database_path

        Returns:
            dict: the input run_args
        """
        # the following args that are optional in the
        # non-concurrent run function are required here
        required_kwargs = [
            "aidb_path", "cbm_exe_path", "results_database_path"]
        for required_kwarg in required_kwargs:
            if required_kwarg not in run_args:
                raise ValueError(f"{required_kwarg} is a required argument")
        results_database_dir = os.path.dirname(
            run_args["results_database_path"])
        if not os.path.exists(results_database_dir):
            os.makedirs(results_database_dir)
        log_path = os.path.splitext(
            run_args["results_database_path"])[0] + ".log"
        loghelper.start_logging(log_path, 'w+', use_console=False)
        with tempfile.TemporaryDirectory() as temp_dir:
            toolbox_env_path = os.path.join(temp_dir, "toolbox")
            toolbox_env.create_toolbox_env(
                self.toolbox_path, toolbox_env_path)

            kwargs = {
                k: v for k, v in run_args.items() if k != "project_path"}
            kwargs["toolbox_installation_dir"] = toolbox_env_path

            # need to make a local copy of the archive index and project db,
            # since the toolbox's dealings with these databases are not
            # threadsafe.
            environment_aidb = os.path.join(
                toolbox_env_path, "admin", "dbs",
                os.path.basename(kwargs["aidb_path"]))
            shutil.copy(
                kwargs["aidb_path"], environment_aidb)
            kwargs["aidb_path"] = environment_aidb

            local_project_db = os.path.join(
                temp_dir, os.path.basename(run_args["project_path"]))
            shutil.copy(run_args["project_path"], local_project_db)
            args = [local_project_db]
            projectsimulator.run(*args, **kwargs)
            run_args["log_path"] = log_path
            return run_args

    def run(self, run_args, max_workers=None):
        """Runs CBM3 simulations as separate processes.

        ** Important Note ** this method must be called from a "main script"
        and cannot be run from interactive prompts.
        See: https://docs.python.org/3/library/multiprocessing.html

        Args:
            run_args (list): list of dictionaries, where each dictionary
                element forms the argument to pass to
                :py:func:`ConcurrentRunner.run_func`
            max_workers (int, optional): Passed to the max_workers arg of:
                py:class:`concurrent.futures.ProcessPoolExecutor
                Defaults to None.

        Yields:
            dict: a dictionary describing the finished task yielded as each
                task is finished.
        """

        with ProcessPoolExecutor(
                max_workers=max_workers) as executor:
            for item in executor.map(self.run_func, run_args):
                yield item

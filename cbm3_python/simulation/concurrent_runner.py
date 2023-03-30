import os
import shutil
import traceback
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import tempfile

from cbm3_python.simulation import toolbox_env
from cbm3_python.simulation import projectsimulator
from cbm3_python.util import loghelper


class ConcurrentRunner:
    def __init__(self, toolbox_path):
        self.toolbox_path = toolbox_path

    def _run_func(self, run_args):
        # the following args that are optional in the
        # non-concurrent run function are required here
        required_kwargs = ["aidb_path", "cbm_exe_path"]
        for required_kwarg in required_kwargs:
            if required_kwarg not in run_args:
                raise ValueError(f"{required_kwarg} is a required argument")

        log_path = None
        if "log_path" in run_args and run_args["log_path"]:
            log_path = run_args["log_path"]

            if not os.path.exists(os.path.dirname(log_path)):
                os.makedirs(os.path.dirname(log_path))
            loghelper.start_logging(
                run_args["log_path"], "w+", use_console=False
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            toolbox_env_path = os.path.join(temp_dir, "toolbox")
            toolbox_env.create_toolbox_env(self.toolbox_path, toolbox_env_path)

            kwargs = {
                k: v
                for k, v in run_args.items()
                # these items are not keyword args for
                # projectsimulator.run
                if k not in ["project_path", "log_path"]
            }
            kwargs["toolbox_installation_dir"] = toolbox_env_path

            # need to make a local copy of the archive index and project db,
            # since the toolbox's dealings with these databases are not
            # threadsafe.
            environment_aidb = os.path.join(
                toolbox_env_path,
                "admin",
                "dbs",
                os.path.basename(kwargs["aidb_path"]),
            )
            shutil.copy(kwargs["aidb_path"], environment_aidb)
            kwargs["aidb_path"] = environment_aidb

            local_project_db = os.path.join(
                temp_dir, os.path.basename(run_args["project_path"])
            )
            shutil.copy(run_args["project_path"], local_project_db)
            args = [local_project_db]
            projectsimulator.run(*args, **kwargs)
            return run_args

    def run_func(self, run_args):
        """Calls :py:func:`cbm3_python.simulation.projectsimulator.run`
        using the specified args. This function also sets up a toolbox
        environment for safely running CBM3 as multiple processes.

        Args:
            run_args (dict): arguments to
                :py:func:`cbm3_python.simulation.projectsimulator.run`
                in dictionary form.  An optional additional value with
                key "log_path" can be specified in the dictionary into
                which each concurrent simulation will log.

        Raises:
            ValueError: raised if particular required arguments have been
                omitted from run args.

                The required arguments are:

                    * aidb_path
                    * cbm_exe_path

        Returns:
            dict: the input run_args
        """

        try:
            output = {"Exception": None}
            output.update(self._run_func(run_args))
            return output
        except Exception:
            output = {"Exception": traceback.format_exc()}
            output.update(run_args)
            return output

    def run(self, run_args, max_workers=None, raise_exceptions=True):
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
            raise_exceptions (bool, optional): If set to true information on
                any exceptions encountered in the list of run args will be
                raised in a RuntimeError.  If false, no exception will be
                raised, but the same error information is returned in the
                resulting task dictionaries in the "Exception" entry. Defaults
                to True.

        Raises:
            RuntimeError: raised if "raise_exceptions" is set to true, and at
                least one of the simulations specified in run_args encountered
                an exception.
        Yields:
            dict: a dictionary describing the finished task yielded as each
                task is finished.
        """
        exceptions = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.run_func, arg) for arg in run_args]
            for future in as_completed(futures):
                item = future.result()
                if raise_exceptions and item["Exception"]:
                    exceptions.append(item)
                yield item

        if exceptions:
            message = os.linesep.join(
                [
                    os.linesep.join(
                        [
                            "",
                            f"Project path: {x['project_path']}",
                            f"Exception: {x['Exception']}",
                        ]
                    )
                    for x in exceptions
                ]
            )
            raise RuntimeError(message)

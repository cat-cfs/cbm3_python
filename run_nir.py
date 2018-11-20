import os, shutil, argparse, datetime, logging
from simulation.nirpathconfig import NIRPathConfig
from simulation.nirsimulator import NIRSimulator
from util import loghelper

def main():
    try:
        logpath = os.path.join(
                    "{0}_{1}.log".format(
                        "nirsimulator",
                        datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S")))
        loghelper.start_logging(logpath, 'w+')
        parser = argparse.ArgumentParser(description="run CBM NIR simulation based on specified configurations")
        parser.add_argument("--nir_base_path_config", help="path to a csv file "
                            "containing the baseline project, and run results "
                            "database paths by project prefix")
        parser.add_argument("--nir_config", help="json formatted file containing nir configuration")

        args = parser.parse_args()
        nir_path_config = NIRPathConfig()
        nir_path_config.load(os.path.abspath(args.nir_base_path_config))
        simulator = NIRSimulator()

    except Exception as ex:
        logging.exception("")

if __name__ == '__main__':
    main()
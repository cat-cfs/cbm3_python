from simulation import NIRPathConfig

def main():
    try:
        logpath = os.path.join(
                    "{0}_{1}.log".format(
                        "nirpathconfig",
                        datetime.datetime.now().strftime("%Y-%m-%d %H_%M_%S")))
        loghelper.start_logging(logpath, 'w+')
        parser = argparse.ArgumentParser(description="generate a csv file "
            "containing validated NIR project paths, and corresponding run "
            "results database paths")
        parser.add_argument("--output_path", help="path to a an output csv file created by this script")
        parser.add_argument("--base_project_dir", help="directory containing projects and results for the specified project_prefixes")
        parser.add_argument("--project_prefixes", help="comma delimited string of project prefixes (ex. 'BCP,AB,NB')
        parser.add_argument("--results_dir", nargs="?",
                            help="the name of the subdirectory that contains CBM run results for each project. "
                                 "If unspecified 'results' is used")
        args = parser.parse_args()

        n = NIRPathConfig();
        n.create(
           output_path = os.path.abspath(args.output_path),
           base_project_dir = os.path.abspath(args.base_project_dir),
           project_prefixes = [x.upper().strip() for x in results_dir.split(",")],
           results_dir="RESULTS")

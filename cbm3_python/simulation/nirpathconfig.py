# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import csv, os, logging
from cbm3_python.util import loghelper

class NIRPathConfig(object):

    def __init__(self):
        pass

    def create(self, output_path, base_project_dir, project_prefixes, results_dir="RESULTS"):
        '''
        helper method to create a csv file containing validated baseline NIR project paths, and rrdb paths
        '''
        with open(output_path, 'wb') as csvfile:
            fieldNames = ["project_prefix", "project_path", "results_path"]
            writer = csv.DictWriter(csvfile, fieldnames= fieldNames)
            writer.writeheader()
            for project_prefix in project_prefixes:
                row = {
                    "project_prefix": project_prefix,
                    "project_path": self.get_base_project_path(base_project_dir, project_prefix),
                    "results_path": self.get_base_run_results_path(base_project_dir, project_prefix, results_dir)
                    }
                writer.writerow(row)


    def load(self, csv_path):
        with open(csv_path) as csvfile:
            reader = csv.DictReader(csvfile)
            return [x for x in reader]


    def get_base_project_path(self, base_project_dir, project_prefix):
        logging.info("looking for {0} project database".format(project_prefix))
        return self.GetAccessDBPathFromDir(
            os.path.join(base_project_dir, project_prefix),
            newest = True)


    def get_base_run_results_path(self, base_project_dir, project_prefix, results_dir):
        logging.info("looking for {0} run results database".format(project_prefix))
        return self.GetAccessDBPathFromDir(
                        os.path.join(base_project_dir,
                        project_prefix, results_dir), newest = True)


    def GetAccessDBPathFromDir(self, dir, newest=False):
        matches = []
        logging.info("searching '{0}' for access databases".format(dir))
        for i in os.listdir(dir):
            if i.lower().endswith(".mdb"):
                match = os.path.join(dir, i)
                logging.info("found '{0}'".format(match))
                matches.append(match)
        if len(matches) == 1:
            return matches[0]
        elif len(matches) > 1 and newest:
            match = sorted(matches, key= lambda x: os.path.getmtime(x), reverse=True)[0]
            logging.info("using newer database '{0}'".format(match))
            return match
        elif len(matches) > 1 and not newest:
            raise AssertionError("found more than one access database.  Directory='{0}'".format(dir))
        else:
            raise AssertionError("expected a dir containing at least one access database, found {0}.  Directory='{1}'"
                                 .format(matchCount, dir))


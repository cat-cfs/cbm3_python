# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

import os
import subprocess
import logging
# Scott - Jun 2014
# wrapper for the disturbance generator for NIR2014 net net

class DisturbanceGeneratorConfig(object):
    """class to help build disturbance generator configs"""
    def __init__(self, exePath, defaultsPath, aidbPath, config, projectTag, projectDBPath):
        self.exePath = exePath
        self.defaultsPath = defaultsPath

        self.aidbPath = aidbPath
        self.config = config
        self.projectTag = projectTag
        self.projectDBPath = projectDBPath

        
    def Run(self):
        cmd ='{0} "{1}" "{2}"'.format(self.exePath,
                    self.BuildXMLConfig(),
                    self.defaultsPath)
        logging.info("running command '{0}'".format(cmd))
        subprocess.check_call(cmd)
        

    def BuildXMLConfig(self):
        path = os.path.join( os.path.dirname(self.projectDBPath), "DistGen_config.xml")
        with open(path,'w+') as f:
            f.write(self.GetDisturbanceGeneratorConfig(
                self.aidbPath, self.config, self.projectTag, self.projectDBPath))
        logging.info("Create disturbance generator config file '{0}'".format(path))
        return path

    def GetDisturbanceGeneratorConfig(self, aidbPath, config, projectTag, projectDBPath):
        dgTaskFragment = \
"""      <DGTask ProjectTag="{0}" IgnoreTask="false">
         <AIDBOverride />
        <ProjectDBPath>{1}</ProjectDBPath>
        <DisturbanceSets>
          <DisturbanceSet DisturbanceClass="{2}" DisturbanceTypeMode="{3}" Enabled="true">
            <DBOverride />
          </DisturbanceSet>
        </DisturbanceSets>
      </DGTask>"""

        tasks = ""
        for item in config:
            distClass = item["DisturbanceClass"]
            disturbanceDBPath = item["DisturbanceDBPath"]
            disturbanceTypeMode = item["DisturbanceTypeMode"]

            t = dgTaskFragment.format(projectTag,projectDBPath,distClass,disturbanceTypeMode, disturbanceDBPath)
            tasks = tasks + t

        disturbanceDBText = "\n".join(
                ('<DisturbanceDB DisturbanceClass="{0}">{1}</DisturbanceDB>'.format(
                        item["DisturbanceClass"],
                        item["DisturbanceDBPath"]) for item in config))
            
        text = \
"""<?xml version="1.0"?>
<DGConfiguration xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <DGTaskSet>
    <NewDistTypeFormatString>{{0}}</NewDistTypeFormatString>
    <ArchiveIndexPath>{0}</ArchiveIndexPath>
    <DisturbanceDBs>
        {1}
    </DisturbanceDBs>
    <DGTasks>
{2}
    </DGTasks>
  </DGTaskSet>
</DGConfiguration>""".format(aidbPath, disturbanceDBText, tasks)

        return text
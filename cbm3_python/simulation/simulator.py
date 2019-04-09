# Copyright (C) Her Majesty the Queen in Right of Canada,
#  as represented by the Minister of Natural Resources Canada

# ---------------------------------------------------------------------------
#
#  Author: MH
#
#  Created on: April 24, 2013
#
#  Purpose: Replicate all the steps of the VisualBuildScript for a complete CBM run
#           (Why? Because I can't get visual build pro to run on the new HP workstation)
#
#  Usage:
#
#
#  Comments:
#
#
#  Updated: History of changes made to the original script
#
#    Who When     What                                   Why
#    --- ----     ----                                   ----
#   Scott Morken, 20131120, Added simulator class, To test some NIR toolbox builds in light of the KF6 extended disturbance accounting issue
#
#
#
# ---------------------------------------------------------------------------
# Import system modules
import logging, os, shutil, subprocess, glob, sys, re


class Simulator(object):
    def __init__(self, executablePath, simID, projectPath, CBMRunDir, toolboxPath):

        # Define relevant paths
        self.simID = simID
        self.ProjectPath = projectPath
        self.ExecutablePath = executablePath
        self.CBMTemp = CBMRunDir
        self.CBMPath = toolboxPath


    def _ignorethese(self, path, name):
        #use this with the copytree command to control what does and does not get copied
        #ignore Access database files
        ignorelist = []
        for item in name:
            if (item.lower().find('.mdb') != -1):
               ignorelist.append(item)
        return ignorelist


    def getDefaultProjectResultsPath(self):
        return os.path.join(self.ProjectPath,str(self.simID))


    def getDefaultResultsPath(self):
        return os.path.join(self.getDefaultProjectResultsPath(), str(self.simID)+'.mdb')

    def CleanupRunDirectory(self):
        logging.info("\n\n Clean up previous runs in " + self.CBMTemp + " \n")
        top = self.CBMTemp
        for root, dirs, files in os.walk(self.CBMTemp, topdown=False):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                if os.path.join(root, d) != top:
                    shutil.rmtree(os.path.join(root, d),ignore_errors=1)
                    #ignore_errors is TRUE. This is dangerous, but otherwise the script fails when it gets to CBMRun

    def CopyToWorkingDir(self, filePath):
        shutil.copy(filePath, self.CBMTemp)

    def CreateMakelistFiles(self):
        logging.info("\n\n Creating make list files...\n")
        cmd = '"' + os.path.join(self.CBMPath, r'createMakelistFiles.exe') + '" ' + str(self.simID)
        logging.info("Command line: " + cmd)
        subprocess.check_call(cmd)


    def copyMakelist(self):
        logging.info("\n\n Copying makelist.exe to Temp dir...\n")
        shutil.copy2(os.path.join(self.ExecutablePath,r'Makelist.exe'), os.path.join(self.CBMTemp, 'Makelist'))

    def runMakelist(self):
        logging.info("\n\n Running make list...\n")
        makelist_path = os.path.join(self.CBMTemp, r'Makelist\Makelist.exe')
        cwd = os.getcwd()
        try:
            os.chdir(os.path.dirname(makelist_path))  # makelist is expecting the current directory to be its location
            cmd = '"'  + makelist_path + '" '
            logging.info("Command line: " + cmd)
            subprocess.check_call(cmd)
        finally:
            os.chdir(cwd) #change back to the original working dir

    def loadMakelistSVLS(self):
        logging.info("\n\n Loading Makelist SVLs...\n")
        cmd = '"' + os.path.join(self.CBMPath, r'MakelistSVLLoader.exe') + '" ' + str(self.simID)
        logging.info("Command line: " + cmd)
        subprocess.check_call(cmd)

    def copyMakelistOutput(self):
        logging.info("\n\n Copying makelist outputs...\n")
        CBMinpath = os.path.join(self.CBMTemp, r'CBMRun\input')
        if os.path.exists(CBMinpath):
            for f in glob.iglob(os.path.join(CBMinpath,'*')):
                os.remove(f)
        else:
            os.makedirs(CBMinpath)

        for f in glob.iglob(os.path.join(self.CBMTemp, r'Makelist\output\*.ini')):
            shutil.copy2(f, CBMinpath)

    def CopySVLFromPreviousRun(self, previousRunCBMOutputDir):
        linebreak='\n'
        logging.info("\n\n Copying svl data from previous CBMRun output dir {0}".format(previousRunCBMOutputDir))
        srcPaths = []
        split = lambda string : re.findall('[\S]+', string)
        previousRunCBMOutputDir = os.path.abspath(previousRunCBMOutputDir)
        for filename in os.listdir(previousRunCBMOutputDir):
            #collect the svl###.dat files
            if filename.startswith("svl") and filename.endswith(".dat"):
                srcPaths.append(os.path.join(previousRunCBMOutputDir, filename))
        for srcpath in srcPaths:
            CBMinpath = os.path.join(self.CBMTemp, r'CBMRun\input')
            newFileName = os.path.join(CBMinpath,"{0}.ini".format(os.path.splitext(os.path.basename(srcpath))[0]))
            with open(srcpath, 'r') as fInput:
                fInput.readline() #skip line 1
                with open(newFileName, 'w') as fOutput:
                    fOutput.write("0 0" + linebreak)
                    for srcline in fInput:
                        tokens = split(srcline)

                        line1 = " ".join(tokens[0:5] + ['0'])
                        softwood = " ".join(tokens[5:18])
                        hardwood = " ".join(tokens[18:31])
                        dom = " ".join(tokens[31:45])
                        cset = " ".join([x for x in tokens[45:55] if x != "-99"])
                        kyotoflags = " ".join(['0', '1', '1990', '0', '0', '0'] if tokens[55:61] == ['0','0','0','0','0','0'] else tokens[55:61])

                        outlines = [line1, softwood, hardwood, dom, "  ".join([cset, kyotoflags])]

                        for outline in outlines:
                            fOutput.write(outline + linebreak)
                        fOutput.write(linebreak) #extra line break


    def CreateCBMFiles(self):
        logging.info("\n\n Creating CBM files...\n")
        cmd = '"' + os.path.join(self.CBMPath, r'createCBMFiles.exe') + '" ' + str(self.simID)
        logging.info("Command line: " + cmd)
        subprocess.check_call(cmd)
        inf = open(os.path.join(self.CBMTemp, r'CBMRun\input\indicate.inf'),'w')
        inf.write('0\n')
        inf.flush()
        inf.close()

    def CopyCBMExecutable(self):
        logging.info("\n\n Copying CBM.exe to Temp dir...\n")
        shutil.copy2(os.path.join(self.ExecutablePath,r'CBM.exe'), os.path.join(self.CBMTemp, r'CBMRun'))

    def RunCBM(self):
        logging.info("\n\n Running CBM...\n")
        cbm_path = os.path.join(self.CBMTemp, r'CBMRun\CBM.exe')
        cwd = os.getcwd()
        try:
            os.chdir(os.path.dirname(cbm_path)) # CBM is expecting the current directory to be its location
            cmd = '"' + cbm_path + '" '
            logging.info("Command line: " + cmd)
            subprocess.check_call(cmd)
        finally:
            os.chdir(cwd) #change back to the original working dir

    def LoadCBMResults(self, output_path=None):
        logging.info("\n\n Loading CBM Results...\n")
        results_path = self.getDefaultResultsPath() \
            if output_path is None else os.path.abspath(output_path)

        cmd = '"' + os.path.join(self.CBMPath, r'LoaderCL.exe') + '" ' + str(self.simID) + ' "' + results_path + '"'
        logging.info("Command line: " + cmd)
        subprocess.check_call(cmd)

    def CopyTempFiles(self, output_dir=None):
        logging.info("\n\n Copying Tempfiles to Project Directory...\n")
        tempfilepath = ""
        if output_dir is None:
            tempfilepath = os.path.join(self.getDefaultProjectResultsPath(),"Tempfiles")
            if os.path.exists(tempfilepath):
                shutil.rmtree(tempfilepath)
        else:
            tempfilepath = os.path.abspath(output_dir)


        shutil.copytree(self.CBMTemp, tempfilepath, ignore=self._ignorethese)

    def DumpMakelistSVLs(self):
        logging.info("\n\n dumping makelist svls...\n")
        cmd = '"' + os.path.join(self.CBMPath, r'DumpMakelistSVL.exe') + '" "' + str(self.simID) + ' "'
        logging.info("Command line: " + cmd)
        subprocess.check_call(cmd)

    def simulate(self, doLoad=True):

        self.CleanupRunDirectory()

        logging.info("Processing " + str(self.ProjectPath) + "...")

        self.CreateMakelistFiles()

        self.copyMakelist()

        self.runMakelist()

        self.loadMakelistSVLS()

        self.copyMakelistOutput()

        self.CreateCBMFiles()

        self.CopyCBMExecutable()

        self.RunCBM()

        if doLoad:
            self.LoadCBMResults()

        self.CopyTempFiles()


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
import logging, os, shutil, subprocess, glob, sys


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
            if (item.lower().find('.mdb') <> -1): 
               ignorelist.append(item)
        return ignorelist


    def getProjectResultsPath(self):
        return os.path.join(self.ProjectPath,str(self.simID))


    def getResultsPath(self):
        return os.path.join(self.getProjectResultsPath(), str(self.simID)+'.mdb')

    def CleanupRunDirectory(self):
        logging.info("\n\n Clean up previous runs in " + self.CBMTemp + " \n")
        top = self.CBMTemp
        for root, dirs, files in os.walk(self.CBMTemp, topdown=False):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                if os.path.join(root, d) <> top:
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
        os.chdir(os.path.dirname(makelist_path))  # makelist is expecting the current directory to be its location
        cmd = '"'  + makelist_path + '" '
        logging.info("Command line: " + cmd)
        subprocess.check_call(cmd)

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
        os.chdir(os.path.dirname(cbm_path)) # CBM is expecting the current directory to be its location
        cmd = '"' + cbm_path + '" '
        logging.info("Command line: " + cmd)
        subprocess.check_call(cmd)

    def LoadCBMResults(self):
        logging.info("\n\n Loading CBM Results...\n")
        
        cmd = '"' + os.path.join(self.CBMPath, r'LoaderCL.exe') + '" ' + str(self.simID) + ' "' + self.getResultsPath() + '"'
        logging.info("Command line: " + cmd)
        subprocess.check_call(cmd)

    def CopyTempFiles(self):
        logging.info("\n\n Copying Tempfiles to Project Directory...\n")
        tempfilepath = os.path.join(self.getProjectResultsPath(),"Tempfiles")
        if os.path.exists(tempfilepath):
            shutil.rmtree(tempfilepath)
         
        shutil.copytree(self.CBMTemp, os.path.join(self.ProjectPath,str(self.simID),"Tempfiles"), ignore=self._ignorethese)

    def DumpMakelistSVLs(self):
        logging.info("\n\n dumping makelist svls...\n")
        cmd = '"' + os.path.join(self.CBMPath, r'DumpMakelistSVL.exe') + '" "' + str(self.simID) + ' "'
        logging.info("Command line: " + cmd)
        subprocess.check_call(cmd)

    def simulate(self, doLoad=True):  
    
        self.CleanupRunDirectory()                    

        logging.info("Processing " + str(self.ProjectPath) + "...")
        #cwd = os.getcwd()
        #os.chdir(self.CBMPath)

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



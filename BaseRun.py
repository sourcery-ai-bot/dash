#!/usr/bin/env python
#
# Base class for managing pDAQ runs

import os, socket, subprocess, sys, threading, time

from ClusterDescription import ClusterDescription

class RunException(Exception): pass

class FlashFileException(RunException): pass
class LaunchException(RunException): pass
class StateException(RunException): pass

class FlasherThread(threading.Thread):
    "Thread which starts and stops flashers during a run"

    def __init__(self, run, data, times, pauseSecs):
        """
        Create a flasher thread (which has not been started)

        run - BaseRun object
        data - flasher description file
        times -list of flasher durations (in seconds)
        pauseSecs - number of seconds to pause between flasher sequences
        """

        super(FlasherThread, self).__init__(name="FlasherThread")
        self.setDaemon(True)

        self.__run = run
        self.__data = data
        self.__times = times
        self.__pauseSecs = pauseSecs

        self.__sem = threading.BoundedSemaphore()

        self.__running = False

    @staticmethod
    def computeRunDuration(times, pauseSecs):
        """
        Compute the number of seconds needed for this flasher run

        times - list of flasher durations (in seconds)
        pauseSecs - number of seconds to pause between flasher sequences
        """
        tot = 0

        for tm in times:
            tot += tm + pauseSecs + 5

        return tot

    def run(self):
        "Body of the flasher thread"
        self.__sem.acquire()
        self.__running = True

        try:
            self.__runBody()
        finally:
            self.__running = False
            self.__sem.release()

    def __runBody(self):
        "Run the flasher sequences"
        for tm in self.__times:
            if not self.__running:
                break

            problem = self.__run.flash(tm, self.__data)

            if problem or not self.__running:
                break

            time.sleep(self.__pauseSecs)

    def stopThread(self):
        "Stop the flasher thread"
        self.__running = False

    def waitForThread(self):
        "Wait for the thread to complete"

        # acquire the semaphore (which the thread will hold until finished)
        #
        self.__sem.acquire()

        # thread must be done now, release the semaphore and return
        #
        self.__sem.release()

class Run(object):
    def __init__(self, mgr, clusterCfg, runCfg, flashName):
        """
        Manage a single run

        mgr - run manager
        clusterCfg - cluster configuration
        runCfg - run configuration
        flashName - flasher description file name
        """
        self.__mgr = mgr
        self.__clusterCfg = clusterCfg
        self.__runCfg = runCfg
        self.__flashData = None
        self.__runKilled = False

        self.__flashThread = None
        self.__lightMode = None

        # __runNum being 0 is considered a safe initializer as per Dave G.
        # it was None which would cause a TypeError on some 
        # error messages
        self.__runNum = 0
        self.__duration = None

        if self.__clusterCfg is None:
            self.__clusterCfg = self.__mgr.getActiveClusterConfig()
            if self.__clusterCfg is None:
                raise RunException("No cluster configuration specified")

        # if pDAQ isn't active or if we need a different cluster config,
        #   kill the current components
        #
        activeCfg = self.__mgr.getActiveClusterConfig()
        if activeCfg is None or activeCfg != self.__clusterCfg:
            self.__mgr.killComponents()
            self.__runKilled = True

        # if necessary, launch the desired cluster configuration
        #
        if self.__runKilled or self.__mgr.isDead():
            self.__mgr.launch(self.__clusterCfg)

        # get absolute path to flasher data file
        #
        if flashName is None:
            self.__flashData = None
        else:
            self.__flashData = self.__flashPath(flashName)

    @staticmethod
    def __flashPath(flashFile):
        """
        Find a flasher file or raise FlashFileException

        flashFile - name of flasher sequence file

        Returns full path for flasher sequence file

        NOTE: Currently, only $PDAQ_HOME/src/test/resources is checked
        """

        if os.path.exists(flashFile):
            return flashFile

        path = os.path.join(os.environ["PDAQ_HOME"], "src", "test",
                            "resources", flashFile)
        if os.path.exists(path):
            return path

        if not flashFile.endswith(".xml"):
            path += ".xml"
            if os.path.exists(path):
                return path

        raise FlashFileException("Flash file '%s' not found" % flashFile)

    def finish(self):
        "clean up after run has ended"
        self.__mgr.stopRun()

        if not self.__mgr.waitForStopped():
            raise RunException("Run %d did not stop" % self.__runNum)

        if self.__flashThread is not None:
            self.__flashThread.waitForThread()

        if self.__lightMode and not self.__mgr.setLightMode(False):
            raise RunException(("Could not set lightMode to dark after run " +
                                " #%d: %s") % (self.__runNum, self.__runCfg))

        self.__runNum = 0

    def start(self, duration, flashTimes=None, flashPause=60, ignoreDB=False):
        """
        Start a run

        duration - number of seconds to run
        flashTimes - list of times (in seconds) to flash
        flashPause - number of seconds to pause between flashing
        ignoreDB - False if the database should be checked for this run config
        """
        # write the run configuration to the database
        #
        if not ignoreDB:
            self.__mgr.updateDB(self.__runCfg)

        # if we'll be flashing, build a thread to start/stop flashers
        #
        self.__lightMode = self.__flashData is not None and \
            flashTimes is not None
        if not self.__lightMode:
            self.__flashThread = None
        else:
            flashDur = FlasherThread.computeRunDuration(flashTimes, flashPause)
            if flashDur > duration:
                if duration > 0:
                    print >>sys.stderr, ("Run length was %d secs, but need" +
                                         " %d secs for flashers") % \
                                         (duration, flashDur)
                duration = flashDur

            self.__flashThread = FlasherThread(self.__mgr, self.__flashData,
                                               flashTimes, flashPause)

        # get the new run number
        #
        self.__runNum = self.__mgr.getLastRunNumber() + 1
        self.__duration = duration

        # set the LID mode
        #
        if not self.__mgr.setLightMode(self.__lightMode):
            raise RunException("Could not set lightMode for run #%d: %s" %
                               (self.__runNum, self.__runCfg))

        # start the run
        #
        if not self.__mgr.startRun(self.__runCfg, duration, 1, ignoreDB):
            raise RunException("Could not start run #%d: %s" %
                               (self.__runNum, self.__runCfg))

        # make sure we've got the correct run number
        #
        curNum = self.__mgr.getRunNumber()
        if curNum != self.__runNum:
            print >>sys.stderr, \
                "  Expected run number %d, but actual number is %s" % \
                (self.__runNum, curNum)
            self.__runNum = curNum

        # print run info
        #
        if self.__flashThread is None:
            runType = "run"
        else:
            runType = "flasher run"

        print "Started %s %d (%d secs) %s" % \
            (runType, self.__runNum, duration, self.__runCfg)

        # start flashing
        #
        if self.__flashThread is not None:
            self.__flashThread.start()

    def stop(self):
        "stop run"
        self.__mgr.stop()

    def wait(self):
        "wait for run to finish"
        self.__mgr.waitForRun(self.__runNum, self.__duration)

class BaseRun(object):
    """User's PATH, used by findExecutable()"""
    PATH = None

    def __init__(self, showCmd=False, showCmdOutput=False, dbType=None):
        """
        showCmd - True if commands should be printed before being run
        showCmdOutput - True if command output should be printed
        """
        self.__showCmd = showCmd
        self.__showCmdOutput = showCmdOutput

        if dbType is not None:
            self.__dbType = dbType
        else:
            self.__dbType = ClusterDescription.getClusterDatabaseType()

        # check for needed executables
        #
        self.__launchProg = self.findExecutable("Launch program", "DAQLaunch.py")

        self.__updateDBProg = \
            os.path.join(os.environ["HOME"], "offline-db-update",
                         "offline-db-update-config")
        if not self.checkExists("PnF program", self.__updateDBProg, False):
            self.__updateDBProg = None

        # make sure run-config directory exists
        #
        self.__configDir = os.path.join(os.environ["PDAQ_HOME"], "config")
        if not os.path.isdir(self.__configDir):
            raise SystemExit("Run config directory '%s' does not exist" %
                             self.__configDir)

    @staticmethod
    def checkExists(name, path, fatal=True):
        """
        Exit if the specified path does not exist

        name - description of this path (used in error messages)
        path - file/directory path
        fatal - True if program should exit if file is not found
        """
        if not os.path.exists(path):
            if fatal:
                raise SystemExit("%s '%s' does not exist" % (name, path))
            return False
        return True

    def cleanUp(self):
        """Do final cleanup before exiting"""
        raise NotImplementedError()

    def createRun(self, clusterCfg, runCfg, flashName=None):
        return Run(self, clusterCfg, runCfg, flashName)

    @classmethod
    def findExecutable(cls, name, cmd):
        """Find 'cmd' in the user's PATH"""
        if cls.PATH is None:
            cls.PATH = os.environ["PATH"].split(":")
        for pdir in cls.PATH:
            pcmd = os.path.join(pdir, cmd)
            if os.path.exists(pcmd):
                return pcmd
        raise SystemExit("%s '%s' does not exist" % (name, cmd))

    def flash(self, tm, data):
        """Start flashers for the specified duration with the specified data"""
        raise NotImplementedError()

    @staticmethod
    def getActiveClusterConfig():
        "Return the name of the current pDAQ cluster configuration"
        clusterFile = os.path.join(os.environ["HOME"], ".active")
        try:
            f = open(clusterFile, "r")
            ret = f.readline()
            f.close()
            return ret.rstrip('\r\n')
        except:
            return None

    def getLastRunNumber(self):
        "Return the last run number"
        raise NotImplementedError()

    def getRunNumber(self):
        "Return the current run number"
        raise NotImplementedError()

    def ignoreDatabase(self):
        return self.__dbType == ClusterDescription.DBTYPE_NONE

    def isDead(self, refreshState=False):
        raise NotImplementedError()

    def isRecovering(self, refreshState=False):
        raise NotImplementedError()

    def isRunning(self, refreshState=False):
        raise NotImplementedError()

    def isStopped(self, refreshState=False):
        raise NotImplementedError()

    def isStopping(self, refreshState=False):
        raise NotImplementedError()

    def killComponents(self):
        "Kill all pDAQ components"
        cmd = "%s -k" % self.__launchProg
        if self.__showCmd: print cmd
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, close_fds=True,
                                shell=True)
        proc.stdin.close()

        failLine = None
        for line in proc.stdout:
            line = line.rstrip()

            if line.startswith("Found "):
                failLine = line
            elif line.find("DAQ is not currently active") > 0:
                pass
            elif line.find("To force a restart") < 0:
                print >>sys.stderr, "KillComponents: %s" % line
        proc.stdout.close()

        proc.wait()

        if failLine is not None:
            raise LaunchException("Could not kill components: %s" % failLine)

    def launch(self, clusterCfg):
        """
        (Re)launch pDAQ with the specified cluster configuration

        clusterCfg - cluster configuration
        """
        if self.isRunning():
            raise LaunchException("There is at least one active run")

        cmd = "%s -c %s -e &" % (self.__launchProg, clusterCfg)
        if self.__showCmd:
            print cmd
        else:
            print "Launching %s" % clusterCfg

        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, close_fds=True,
                                shell=True)
        proc.stdin.close()

        for line in proc.stdout:
            line = line.rstrip()
            if self.__showCmdOutput: print '+ ' + line

        proc.stdout.close()

        proc.wait()

        # give components a chance to start
        time.sleep(5)

    def run(self, clusterCfg, runCfg, duration, flashName=None,
            flashTimes=None, flashPause=60, ignoreDB=False):
        """
        Manage a set of runs

        clusterCfg - cluster configuration
        runCfg - run configuration
        duration - number of seconds to run
        numRuns - number of runs (default=1)
        flashName - flasher description file name
        flashTimes - list of times (in seconds) to flash
        flashPause - number of seconds to pause between flashing
        ignoreDB - False if the database should be checked for this run config
        """
        run = self.createRun(clusterCfg, runCfg, flashName)
        run.start(duration, flashTimes, flashPause, ignoreDB)
        try:
            run.wait()
        finally:
            run.finish()

    def setLightMode(self, isLID):
        """
        Set the Light-In-Detector mode

        isLID - True for light-in-detector mode, False for dark mode

        Return True if the light mode was set successfully
        """
        raise NotImplementedError()

    def startRun(self, runCfg, duration, numRuns=1, ignoreDB=False):
        """
        Start a run

        runCfg - run configuration file name
        duration - number of seconds for run
        numRuns - number of runs (default=1)
        ignoreDB - don't check the database for this run config

        Return True if the run was started
        """
        raise NotImplementedError()

    def state(self):
        """Current state of runset"""
        raise NotImplementedError()

    def stopRun(self):
        """Stop the run"""
        raise NotImplementedError()

    def updateDB(self, runCfg):
        """
        Add this run configuration to the database

        runCfg - run configuration
        """
        if self.__dbType == ClusterDescription.DBTYPE_NONE:
            return

        if self.__updateDBProg is None:
            print >>sys.stderr, "Not updating database with \"%s\"" % runCfg
            return

        runCfgPath = os.path.join(self.__configDir, runCfg + ".xml")
        self.checkExists("Run configuration", runCfgPath)

        if self.__dbType == ClusterDescription.DBTYPE_TEST:
            arg = "-D I3OmDb_test"
        else:
            arg = ""

        cmd = "%s %s %s" % (self.__updateDBProg, arg, runCfgPath)
        if self.__showCmd: print cmd
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, close_fds=True,
                                shell=True)
        proc.stdin.close()

        for line in proc.stdout:
            line = line.rstrip()
            if self.__showCmdOutput: print '+ ' + line

            if line.find("ErrAlreadyExists") > 0:
                continue

            elif line != "xml":
                print >>sys.stderr, "UpdateDB: %s" % line
        proc.stdout.close()

        proc.wait()

    def waitForRun(self, runNum, duration):
        """
        Wait for the current run to start and stop

        runNum - current run number
        duration - expected number of seconds this run will last
        """

        # wake up every 'waitSecs' seconds to check run state
        #
        waitSecs = 10

        numTries = duration / waitSecs
        numWaits = 0

        while True:
            if not self.isRunning():
                runTime = numWaits * waitSecs
                if runTime < duration:
                    print >>sys.stderr, \
                        ("WARNING: Expected %d second run, but run %d" +
                         " ended after about %d seconds") % \
                         (duration, runNum, runTime)

                if self.isStopped(False) or \
                        self.isStopping(False) or \
                        self.isRecovering(False):
                    break

                print >>sys.stderr, "Unexpected run %d state %s" % \
                    (runNum, self.state())

            numWaits += 1
            if numWaits > numTries:
                break

            time.sleep(waitSecs)

    def waitForStopped(self):
        """Wait for the current run to be stopped"""
        raise NotImplementedError()

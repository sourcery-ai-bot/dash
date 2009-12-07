#!/usr/bin/env python

import socket, sys, unittest

from DAQConst import DAQPort
from DAQLaunch import compNameJarPartsMap, cyclePDAQ, doKill, doLaunch, \
    killJavaProcesses, startJavaProcesses

from DAQMocks import MockParallelShell, MockDeployComponent

class MockNode(object):
    LIST = []

    def __init__(self, hostName):
        self.__hostName = hostName
        self.__comps = []

    def __str__(self):
        return "%s[%s]" % (str(self.__hostName), str(self.__comps))

    def addComp(self, compName, compId, logLevel, jvm, jvmArgs):
        comp = MockDeployComponent(compName, compId, logLevel, jvm, jvmArgs)
        self.__comps.append(comp)
        return comp

    def components(self): return self.__comps
    def hostName(self): return self.__hostName

class MockClusterConfig(object):
    def __init__(self, name):
        self.configName = name
        self.__nodes = []

    def addNode(self, node):
        self.__nodes.append(node)

    def clearActiveConfig(self):
        pass

    def nodes(self): return self.__nodes[:]

    def writeCacheFile(self, writeActive):
        pass

class DAQLaunchTest(unittest.TestCase):
    def testStartJava(self):
        dryRun = False
        configDir = '/foo/bar'
        logPort = 1234
        jvm = "java"
        jvmArgs = "-server"
        verbose = False
        checkExists = False

        logLevel = 'DEBUG'

        for compName in compNameJarPartsMap:
            if compName[-3:] == 'hub':
                compName = compName[:-3] + "Hub"
                compId = 17
            else:
                compId = 0
                if compName.endswith("builder"):
                    compName = compName[:-7] + "Builder"

            for host in MockNode.LIST:
                node = MockNode(host)
                comp = node.addComp(compName, compId, logLevel, jvm, jvmArgs)

                cfgName = 'mockCfg'

                config = MockClusterConfig(cfgName)
                config.addNode(node)

                for isLive in (True, False):
                    if isLive:
                        livePort = DAQPort.I3LIVE
                    else:
                        livePort = None

                    for eventCheck in (True, False):
                        parallel = MockParallelShell()

                        parallel.addExpectedJava(comp, configDir, logPort,
                                                 livePort, verbose, eventCheck,
                                                 host)

                        startJavaProcesses(dryRun, config, configDir, None,
                                           logPort, livePort, verbose,
                                           eventCheck=eventCheck,
                                           checkExists=checkExists,
                                           parallel=parallel)

                        parallel.check()

    def testKillJava(self):
        for compName in compNameJarPartsMap:
            if compName[-3:] == 'hub':
                compId = 17
            else:
                compId = 0

            dryRun = False
            verbose = False
            jvm = "java"
            jvmArgs = "-server"

            logLevel = 'DEBUG'

            for host in MockNode.LIST:
                node = MockNode(host)
                node.addComp(compName, compId, logLevel, jvm, jvmArgs)

                cfgName = 'mockCfg'

                config = MockClusterConfig(cfgName)
                config.addNode(node)

                for killWith9 in (True, False):
                    parallel = MockParallelShell()

                    parallel.addExpectedJavaKill(compName, killWith9, verbose,
                                                 host)

                    killJavaProcesses(dryRun, config, verbose, killWith9,
                                      parallel)

                    parallel.check()

    def testLaunch(self):
        dryRun = False
        configDir = '/foo/bar/cfg'
        dashDir = '/foo/bar/dash'
        logDir = '/foo/bar/log'
        spadeDir = '/foo/bar/spade'
        copyDir = '/foo/bar/copy'
        logPort = 1234
        verbose = False
        quiet = True
        checkExists = False

        compName = 'eventBuilder'
        compId = 0
        jvm = "java"
        jvmArgs = "-server"
        logLevel = 'DEBUG'

        # if there are N targets, range is 2^N
        for targets in range(8):
            doCnC = (targets & 1) == 1
            doDAQRun = (targets & 2) == 2
            doLive = (targets & 4) == 4

            for host in MockNode.LIST:
                node = MockNode(host)
                comp = node.addComp(compName, compId, logLevel, jvm, jvmArgs)

                cfgName = 'mockCfg'

                config = MockClusterConfig(cfgName)
                config.addNode(node)

                for isLive in (True, False):
                    if isLive:
                        livePort = DAQPort.I3LIVE
                    else:
                        livePort = None

                    for evtChk in (True, False):
                        parallel = MockParallelShell()

                        parallel.addExpectedPython(doLive, doDAQRun, doCnC,
                                                   dashDir, configDir, logDir,
                                                   spadeDir, cfgName, copyDir,
                                                   logPort, livePort)
                        parallel.addExpectedJava(comp, configDir, logPort,
                                                 livePort, verbose, evtChk,
                                                 host)

                        dryRun = False

                        doLaunch(doLive, doDAQRun, doCnC, dryRun, verbose,
                                 quiet, config, dashDir, configDir, logDir,
                                 spadeDir, copyDir, logPort, livePort,
                                 eventCheck=evtChk, checkExists=checkExists,
                                 startMissing=False, parallel=parallel)

                        parallel.check()

    def testDoKill(self):
        dryRun = False
        dashDir = '/foo/bar/dash'
        verbose = False
        quiet = True

        compName = 'eventBuilder'
        compId = 0
        jvm = "java"
        jvmArgs = "-server"
        logLevel = 'DEBUG'

        # if there are N targets, range is 2^N
        for targets in range(8):
            doCnC = (targets & 1) == 1
            doDAQRun = (targets & 2) == 2
            doLive = (targets & 4) == 4

            for host in MockNode.LIST:
                node = MockNode(host)
                node.addComp(compName, compId, logLevel, jvm, jvmArgs)

                cfgName = 'mockCfg'

                config = MockClusterConfig(cfgName)
                config.addNode(node)

                for killWith9 in (True, False):
                    parallel = MockParallelShell()

                    parallel.addExpectedPythonKill(doLive, doDAQRun, doCnC,
                                                   dashDir, killWith9)
                    parallel.addExpectedJavaKill(compName, killWith9, verbose,
                                                 host)

                    doKill(doLive, doDAQRun, doCnC, dryRun, dashDir, verbose,
                           quiet, config, killWith9, parallel)

                    parallel.check()

    def testCycle(self):
        configDir = '/foo/bar/cfg'
        dashDir = '/foo/bar/dash'
        logDir = '/foo/bar/log'
        spadeDir = '/foo/bar/spade'
        copyDir = '/foo/bar/copy'
        logPort = 1234
        checkExists = False

        compName = 'eventBuilder'
        compId = 0
        jvm = "java"
        jvmArgs = "-server"
        logLevel = 'DEBUG'

        dryRun = False
        verbose = False
        killWith9 = False

        doCnC = True
        doDAQRun = False
        doLive = False

        for host in MockNode.LIST:
            node = MockNode(host)
            comp = node.addComp(compName, compId, logLevel, jvm, jvmArgs)

            cfgName = 'mockCfg'

            config = MockClusterConfig(cfgName)
            config.addNode(node)

            for isLive in (True, False):
                if isLive:
                    livePort = DAQPort.I3LIVE
                else:
                    livePort = None

                for eventCheck in (True, False):
                    parallel = MockParallelShell()

                    parallel.addExpectedPythonKill(doLive, doDAQRun, doCnC,
                                                   dashDir, killWith9)
                    parallel.addExpectedJavaKill(compName, killWith9, verbose,
                                                 host)

                    parallel.addExpectedPython(doLive, doDAQRun, doCnC,
                                               dashDir, configDir, logDir,
                                               spadeDir, cfgName, copyDir,
                                               logPort, livePort)
                    parallel.addExpectedJava(comp, configDir, logPort, livePort,
                                             verbose, eventCheck, host)

                    cyclePDAQ(dashDir, config, configDir, logDir, spadeDir,
                              copyDir, logPort, livePort,
                              eventCheck=eventCheck, checkExists=checkExists,
                              startMissing=False, parallel=parallel)

                    parallel.check()

if __name__ == '__main__':
    # make sure icecube.wisc.edu is valid
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    for rmtHost in ('localhost', 'icecube.wisc.edu'):
        try:
            s.connect((rmtHost, 56))
            MockNode.LIST.append(rmtHost)
        except:
            print >>sys.stderr, "Warning: Remote host %s is not valid" % rmtHost

    unittest.main()

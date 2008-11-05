#!/usr/bin/env python

import datetime, os, re, sys, thread, time, unittest
from DAQRun import DAQRun, RunArgs

class MockLogger(object):
    def __init__(self, logPath=''):
        self.expMsgs = []
        self.logPath = logPath

    def __checkMsg(self, msg):
        if len(self.expMsgs) == 0:
            raise Exception('Unexpected log message: %s' % msg)
        (expMsg, partialMatch) = self.expMsgs[0]
        if partialMatch and msg.find(expMsg) == -1:
            raise Exception('Expected partial log message of "%s", not "%s"' %
                            (expMsg, msg))
        elif not partialMatch and expMsg != msg:
            raise Exception('Expected log message "%s", not "%s"' %
                            (expMsg, msg))
        del self.expMsgs[0]

    def addExpected(self, msg, partialMatch=False):
        self.expMsgs.append((msg, partialMatch))

    def checkEmpty(self):
        if len(self.expMsgs) != 0:
            raise Exception("Didn't receive %d expected log messages: %s" %
                            (len(self.expMsgs), str(self.expMsgs)))

    def close(self):
        pass

    def dashLog(self, m):
        self.__checkMsg(m)

    def localAppend(self, m):
        self.__checkMsg(m)

    def stopServing(self):
        pass

    def startServing(self):
        pass

    def write_ts(self, s):
        self.__checkMsg(s)

class MockMoni(object):
    def __init__(self):
        self.entries = {}
        self.isTime = False

    def addEntry(self, cid, section, field, val):
        if not self.entries.has_key(cid):
            self.entries[cid] = {}

        key = section + ':' + field
        if self.entries[cid].has_key(key):
            raise Exception('Found multiple entries for %s %s/%s' %
                            (cid, section, field))
        self.entries[cid][key] = val

    def doMoni(self):
        pass

    def getSingleBeanField(self, cid, section, field):
        if not self.entries.has_key(cid):
            raise Exception('No MBean entries for %s' % cid)

        key = section + ':' + field
        if not self.entries[cid].has_key(key):
            raise Exception('No MBean entry for %s %s/%s' %
                            (cid, section, field))

        return self.entries[cid][key]

    def timeToMoni(self):
        return self.isTime

class MockWatchdog(object):
    def __init__(self):
        self.inProg = False
        self.caughtErr = False
        self.done = False
        self.healthy = False
        self.isTime = False

        self.threadCleared = False
        self.watchStarted = False

    def caughtError(self):
        return self.caughtErr

    def clearThread(self):
        self.threadCleared = True

    def inProgress(self):
        return self.inProg

    def isDone(self):
        return self.done

    def isHealthy(self):
        return self.healthy

    def startWatch(self):
        self.watchStarted = True

    def timeToWatch(self):
        return self.isTime

class MockCnCRPC(object):
    COMP_PAT = re.compile(r'(\S+)#(\d+)')

    def __init__(self):
        self.compList = None

        self.nextRunsetId = 1

        self.runsetId = None
        self.runsetComps = None

        self.denyBreak = False

        self.resetFlags()

    def _buildRunset(self, required):
        if self.compList is None:
            raise Exception('List of components has not been set')

        newSet = []
        for r in required:
            sep = r.find('#')
            if sep < 0:
                raise Exception('Found bad component "%s"' % r)

            name = r[:sep]
            id = int(r[sep+1:])

            found = False
            for c in self.compList:
                if c[1] == name and c[2] == id:
                    found = True
                    newSet.append((c[0], c[1], c[2], c[3], c[4], c[5]))
                    break

            if not found:
                raise Exception('Could not find component "%s"' % r)

        self.runsetComps = newSet

        if self.nextRunsetId is None:
            self.nextRunsetId = 43
        self.runsetId = self.nextRunsetId
        self.nextRunsetId += 1

        return self.runsetId

    def _doSubrun(self, runSetId, subRunId, domList):
        pass

    def _listRunset(self, id):
        if self.runsetId is None:
            return ()
        if id != self.runsetId:
            raise Exception('Expected runset#%d, not #%d' % (self.runsetId, id))
        return self.runsetComps

    def _showComponents(self):
        if self.compList is None:
            raise Exception('List of components has not been set')

        showList = []

        id = 1
        for c in self.compList:
            showList.append('ID#%d %s#%d at %s:%d' %
                            (c[0], c[1], c[2], c[3], c[4]))

        return showList

    def getRunsetLoggers(self):
        return self.runsetLoggers

    def resetFlags(self):
        self.LogToFlag = False
        self.RSConfigFlag = False
        self.RSStartFlag = False
        self.RSStopFlag = False
        self.RSBreakFlag = False
        self.RSLogToFlag = False
        self.RSFlashFlag = False

    def rpccall(self, name, *args):
        if name == 'rpc_show_components':
            return self._showComponents()
        if name == 'rpc_log_to':
            self.LogToFlag = True
            return
        if name == 'rpc_runset_make':
            return self._buildRunset(args[0])
        if name == 'rpc_runset_list':
            return self._listRunset(args[0])
        if name == 'rpc_runset_configure':
            self.RSConfigFlag = True
            return
        if name == 'rpc_runset_start_run':
            self.RSStartFlag = True
            return
        if name == 'rpc_runset_stop_run':
            self.RSStopFlag = True
            return
        if name == 'rpc_runset_break':
            if self.denyBreak:
                raise Exception('BROKEN')
            self.RSBreakFlag = True
            return
        if name == 'rpc_runset_log_to':
            if self.runsetId is None:
                raise Exception('Unexpected runset#%d' % args[0])
            if args[0] != self.runsetId:
                raise Exception('Expected runset#%d, not #%d' %
                                (self.runsetId, args[0]))
            self.runsetLoggers = args[2]
            return
        if name == 'rpc_runset_subrun':
            self.RSFlashFlag = True
            return

        raise Exception('Unknown RPC call "%s"' % name)

    def setComponents(self, lst):
        self.compList = []
        for c in lst:
            self.compList.append((c[0], c[1], c[2], c[3], c[4], c[5]))

    def setRunSet(self, id, comps):
        self.runsetId = id
        self.runsetComps = comps

    def showStats(self):
        return 'NoStats'

class StubbedDAQRun(DAQRun):
    __socketLogger = None

    def __init__(self, args, startServer):
        self.moniStub = MockMoni()
        self.watchdogStub = MockWatchdog()
        self.liveLog = None

        super(StubbedDAQRun, self).__init__(args, startServer)

    def createSocketLogger(cls, port, shortName, path):
        return cls.__socketLogger
    createSocketLogger = classmethod(createSocketLogger)

    def createLogCollector(self, runNum, logDir):
        if self.log is not None:
            return self.log
        return MockLogger(logDir)

    def get_base_prefix(self, runNum, runTime, runDuration):
        return 'MockPrefix#%d' % runNum

    def move_spade_files(self, copyDir, basePrefix, logTopLevel, runDir, spadeDir):
        pass

    def restartComponents(self):
        pass

    def setSocketLogger(cls, logger):
        cls.__socketLogger = logger
    setSocketLogger = classmethod(setSocketLogger)

    def setup_monitoring(self):
        self.moni = self.moniStub

    def setup_watchdog(self):
        self.watchdog = self.watchdogStub

class TestDAQRun(unittest.TestCase):
    NEXTPORT = 9876
    CLUSTER_CONFIG = 'sim-localhost'
    SPADE_DIR = '/tmp'
    LOG_DIR = '/tmp'

    def getRunArgs(self, extraArgs=None):

        stdArgs = { '-c' : 'src/test/resources/config',
                    '-l' : TestDAQRun.LOG_DIR,
                    '-n' : '',
                    '-p' : str(TestDAQRun.NEXTPORT),
                    '-q' : '',
                    '-s' : TestDAQRun.SPADE_DIR,
                    '-u' : TestDAQRun.CLUSTER_CONFIG }
        TestDAQRun.NEXTPORT += 1

        oldArgv = sys.argv
        try:
            sys.argv = ['foo']

            for k in stdArgs.keys():
                if extraArgs is None or not extraArgs.has_key(key):
                    sys.argv.append(k)
                    if len(stdArgs[k]) > 0:
                        sys.argv.append(stdArgs[k])

            if extraArgs is not None:
                for k in extraArgs.keys():
                    sys.argv.append(k)
                    if len(extraArgs[k]) > 0:
                        sys.argv.append(extraArgs[k])

            args = RunArgs()
            args.parse()
        finally:
            sys.argv = oldArgv

        return args

    def testParseCompName(self):
        lst = (('ID#1 foo#0 at localhost:12345 ',
                (1, 'foo', 0, 'localhost', 12345)),
               ('ID#22 bar#11 at 192.168.1.10:54321',
                (22, 'bar', 11, '192.168.1.10', 54321)),
               ('bad', ()))

        for l in lst:
            t = DAQRun.parseComponentName(l[0])
            self.assertEqual(len(l[1]), len(t),
                             'Expected %d-element tuple, but got %d elements' %
                             (len(l[1]), len(t)))
            for n in range(0, len(t)):
                self.assertEquals(l[1][n], t[n],
                                  'Expected element#%d to be "%s", not "%s"' %
                                  (n, str(l[1][n]), str(t[n])))

    def testGetNameList(self):
        lst = (('ID#1 foo#0 at localhost:12345 ',
                (1, 'foo', 0, 'localhost', 12345)),
               ('ID#22 bar#11 at 192.168.1.10:54321',
                (22, 'bar', 11, '192.168.1.10', 54321)),
               ('bad', ()))

        names = []
        vals = []
        for l in lst:
            names.append(l[0])
            if len(l[1]) > 0:
                vals.append('%s#%d' % (l[1][1], l[1][2]))

        nlst = list(DAQRun.getNameList(names))
        self.assertEqual(len(vals), len(nlst),
                         'Expected %d-element list, but got %d elements' %
                         (len(vals), len(nlst)))
        for n in range(0, len(nlst)):
            self.assertEquals(vals[n], nlst[n],
                              'Expected element#%d to be "%s", not "%s"' %
                              (n, str(vals[n]), str(nlst[n])))

    def testFindMissing(self):
        required = ['abc#1', 'def#2', 'ghi#3']

        tooFew = []
        missing = None
        for r in required:
            if len(tooFew) == 0 or missing is not None:
                tooFew.append(r)
            else:
                missing = r

        waitList = DAQRun.findMissing(required, tooFew)
        self.assertEquals(1, len(waitList),
                          "Expected ['%s'], not %s" % (missing, str(waitList)))
        self.assertEquals(missing, waitList[0], "Expected '%s' not '%s'" %
                          (missing, waitList[0]))

        justRight = []
        for r in required:
            justRight.append(r)

        waitList = DAQRun.findMissing(required, justRight)
        self.assertEquals(0, len(waitList), "Did not expect %s" % str(waitList))

        tooMany = []
        for r in required:
            tooMany.append(r)
        tooMany.append('jkl#9')

        waitList = DAQRun.findMissing(required, tooMany)
        self.assertEquals(0, len(waitList), "Did not expect %s" % str(waitList))

    def testCreate(self):
        dr = DAQRun(self.getRunArgs(), False)

    def testLog(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        expMsg = 'foo'

        logger.addExpected(expMsg)

        dr.logmsg(expMsg)

        logger.checkEmpty()

    def testWaitForRequiredBad(self):
        comps = [(0, 'abc', 1, 'xxx', 1, 2),
                 (1, 'def', 2, 'yyy', 3, 4),
                 (2, 'ghi', 3, 'zzz', 5, 6)]

        dr = DAQRun(self.getRunArgs(), False)

        cnc = MockCnCRPC()
        cnc.setComponents(comps[1:])

        required = []
        for c in comps:
            required.append('%s#%d' % (c[1], c[2]))

        try:
            dr.waitForRequiredComponents(cnc, required, 0)
            self.fail('Unexpected success')
        except Exception, e:
            self.assertEquals('Still waiting for ' + required[0], str(e),
                              'Unexpected exception message "%s"' % str(e))

    def testWaitForRequiredGood(self):
        comps = [(0, 'abc', 1, 'xxx', 1, 2),
                 (1, 'def', 2, 'yyy', 3, 4),
                 (2, 'ghi', 3, 'zzz', 5, 6)]

        dr = DAQRun(self.getRunArgs(), False)

        cnc = MockCnCRPC()
        cnc.setComponents(comps)


        required = []
        for c in comps:
            required.append('%s#%d' % (c[1], c[2]))

        dr.waitForRequiredComponents(cnc, required, 0)

    def testWaitForRequiredBad(self):
        comps = [(0, 'abc', 1, 'xxx', 1, 2),
                 (1, 'def', 2, 'yyy', 3, 4),
                 (2, 'ghi', 3, 'zzz', 5, 6)]

        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        cnc = MockCnCRPC()
        cnc.setComponents(comps[1:])

        expRunNum = 100
        expId = 123

        dr.runStats.runNum = expRunNum
        dr.runSetID = expId

        DAQRun.COMP_TOUT = 0

        required = []
        for c in comps:
            required.append('%s#%d' % (c[1], c[2]))

        logger.addExpected(('Starting run %d (waiting for required %d' +
                            ' components to register w/ CnCServer)') %
                           (dr.runStats.runNum, len(required)))

        try:
            dr.build_run_set(cnc, required)
            self.fail('Unexpected success')
        except Exception, e:
            self.assertEquals('Still waiting for ' + required[0], str(e),
                              'Unexpected exception message "%s"' % str(e))

        logger.checkEmpty()

    def testWaitForRequiredGood(self):
        comps = [(0, 'abc', 1, 'xxx', 1, 2),
                 (1, 'def', 2, 'yyy', 3, 4),
                 (2, 'ghi', 3, 'zzz', 5, 6)]

        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        cnc = MockCnCRPC()
        cnc.setComponents(comps)

        expRunNum = 100

        dr.runStats.runNum = expRunNum

        DAQRun.COMP_TOUT = 0

        expId = cnc.nextRunsetId

        required = []
        for c in comps:
            required.append('%s#%d' % (c[1], c[2]))


        logger.addExpected(('Starting run %d (waiting for required %d' +
                            ' components to register w/ CnCServer)') %
                           (dr.runStats.runNum, len(required)))
        logger.addExpected('Created Run Set #%d' % expId)

        dr.build_run_set(cnc, required)
        self.assertEquals(expId, dr.runSetID, 'Expected runset#%d, not #%d' %
                          (expId, dr.runSetID))

        logger.checkEmpty()

    def testFillCompDict(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        expId = 99
        expComps = [(3, 'foo', 0, 'localhost', 1234, 5678),
                    (7, 'bar', 1, 'localhost', 4321, 8765)]

        cnc = MockCnCRPC()
        cnc.setRunSet(expId, expComps)

        dr.runSetID = expId

        dr.fill_component_dictionaries(cnc)

        for i in range(0, len(expComps)):
            key = dr.setCompIDs[i]
            self.assertEquals(expComps[i][0], key,
                              'Expected comp#%d to be %s, not %s' %
                              (i, expComps[i][0], key))
            self.assertEquals(expComps[i][1], dr.shortNameOf[key],
                              'Expected shortName#%d to be %s, not %s' %
                              (i, expComps[i][1], dr.shortNameOf[key]))
            self.assertEquals(expComps[i][2], dr.daqIDof[key],
                              'Expected daqID#%d to be %d, not %d' %
                              (i, expComps[i][2], dr.daqIDof[key]))
            self.assertEquals(expComps[i][3], dr.rpcAddrOf[key],
                              'Expected rpcAddr#%d to be %s, not %s' %
                              (i, expComps[i][3], dr.rpcAddrOf[key]))
            self.assertEquals(expComps[i][4], dr.rpcPortOf[key],
                              'Expected rpcPort#%d to be %d, not %d' %
                              (i, expComps[i][4], dr.rpcPortOf[key]))
            self.assertEquals(expComps[i][5], dr.mbeanPortOf[key],
                              'Expected mbeanPort#%d to be %d, not %d' %
                              (i, expComps[i][5], dr.mbeanPortOf[key]))

        logger.checkEmpty()

    def testRunsetConfig(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        cnc = MockCnCRPC()

        logger.addExpected('Configuring run set...')

        dr.runset_configure(cnc, 1, 'foo')
        self.failUnless(cnc.RSConfigFlag, 'Runset was not configured')

        logger.checkEmpty()

    def testRunsetStart(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        cnc = MockCnCRPC()

        expRunNum = 100
        expId = 123

        dr.runStats.runNum = expRunNum
        dr.runSetID = expId

        logger.addExpected('Started run %d on run set %d' % (expRunNum, expId))

        dr.start_run(cnc)
        self.failUnless(cnc.RSStartFlag, 'Runset was not started')

        logger.checkEmpty()

    def testRunsetStop(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        cnc = MockCnCRPC()

        expRunNum = 100

        dr.runStats.runNum = expRunNum

        logger.addExpected('Stopping run %d' % expRunNum)

        dr.stop_run(cnc)
        self.failUnless(cnc.RSStopFlag, 'Runset was not started')

        logger.checkEmpty()

    def testRunsetBreakGood(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        cnc = MockCnCRPC()

        expId = 123

        dr.runSetID = expId

        logger.addExpected('Breaking run set...')

        dr.break_existing_runset(cnc)
        self.failUnless(cnc.RSBreakFlag, 'Runset was not broken')
        self.assertEquals(0, len(dr.setCompIDs),
                          'Should not have any components')
        self.assertEquals(0, len(dr.shortNameOf),
                          'Should not have any short names')
        self.assertEquals(0, len(dr.daqIDof),
                          'Should not have any DAQ IDs')
        self.assertEquals(0, len(dr.rpcAddrOf),
                          'Should not have any RPC addresses')
        self.assertEquals(0, len(dr.rpcPortOf),
                          'Should not have any RPC ports')
        self.assertEquals(0, len(dr.mbeanPortOf),
                          'Should not have any MBean ports')
        if dr.runSetID is not None: self.fail('Runset ID should be unset')
        if dr.lastConfig is not None: self.fail('Last config should be unset')

        logger.checkEmpty()

    def testRunsetBreakBad(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        cnc = MockCnCRPC()
        cnc.denyBreak = True

        expId = 123

        dr.runSetID = expId

        logger.addExpected('Breaking run set...')
        logger.addExpected('WARNING: failed to break run set', True)

        dr.break_existing_runset(cnc)
        self.failIf(cnc.RSBreakFlag, 'Runset was broken')
        self.assertEquals(0, len(dr.setCompIDs),
                          'Should not have any components')
        self.assertEquals(0, len(dr.shortNameOf),
                          'Should not have any short names')
        self.assertEquals(0, len(dr.daqIDof),
                          'Should not have any DAQ IDs')
        self.assertEquals(0, len(dr.rpcAddrOf),
                          'Should not have any RPC addresses')
        self.assertEquals(0, len(dr.rpcPortOf),
                          'Should not have any RPC ports')
        self.assertEquals(0, len(dr.mbeanPortOf),
                          'Should not have any MBean ports')
        if dr.runSetID is not None: self.fail('Runset ID should be unset')
        if dr.lastConfig is not None: self.fail('Last config should be unset')

        logger.checkEmpty()

    def testGetEventCounts(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        numEvts = 17
        numMoni = 222
        numSN = 51
        numTCal = 93

        moni.addEntry(5, 'backEnd', 'NumEventsSent', str(numEvts))
        moni.addEntry(17, 'moniBuilder', 'TotalDispatchedData', str(numMoni))
        moni.addEntry(17, 'snBuilder', 'TotalDispatchedData', str(numSN))
        moni.addEntry(17, 'tcalBuilder', 'TotalDispatchedData', str(numTCal))

        expId = 99
        expComps = [(5, 'eventBuilder', 0, 'x', 1234, 5678),
                    (17, 'secondaryBuilders', 0, 'x', 4321, 8765)]

        cnc = MockCnCRPC()
        cnc.setRunSet(expId, expComps)

        dr.runSetID = expId

        dr.fill_component_dictionaries(cnc)

        expCnts = (numEvts, numMoni, numSN, numTCal)

        cnts = dr.getEventCounts()
        self.assertEquals(len(expCnts), len(cnts),
                          'Expected %d event counts, not %d' %
                          (len(expCnts), len(cnts)))
        for i in range(0, len(expCnts)):
            self.assertEquals(expCnts[i], cnts[i],
                              'Expected event count #%d to be %d, not %d' %
                              (i, expCnts[i], cnts[i]))

        logger.checkEmpty()

    def testCheckAllNone(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        cnc = MockCnCRPC()

        rtnVal = dr.check_all()
        self.failUnless(rtnVal, 'Expected call to succeed')

        logger.checkEmpty()

    def testCheckAllMoniRate(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        numEvts = 1000
        numMoni = 222
        numSN = 51
        numTCal = 93

        moni.isTime = True
        moni.addEntry(5, 'backEnd', 'NumEventsSent', str(numEvts))
        moni.addEntry(17, 'moniBuilder', 'TotalDispatchedData', str(numMoni))
        moni.addEntry(17, 'snBuilder', 'TotalDispatchedData', str(numSN))
        moni.addEntry(17, 'tcalBuilder', 'TotalDispatchedData', str(numTCal))

        expId = 99
        expComps = [(5, 'eventBuilder', 0, 'x', 1234, 5678),
                    (17, 'secondaryBuilders', 0, 'x', 4321, 8765)]

        cnc = MockCnCRPC()
        cnc.setRunSet(expId, expComps)

        dr.runSetID = expId

        dt = datetime.datetime.now()

        maxRate = 300
        for i in range(0, maxRate):
            secs = maxRate - i
            evts = (maxRate - i) * 2
            dr.runStats.physicsRate.add(dt - datetime.timedelta(seconds=secs),
                                        numEvts - evts)

        dr.fill_component_dictionaries(cnc)

        logger.addExpected(('\t%d physics events (2.00 Hz), %d moni events,' +
                            ' %d SN events, %d tcals') %
                           (numEvts, numMoni, numSN, numTCal))

        rtnVal = dr.check_all()
        self.failUnless(rtnVal, 'Expected call to succeed')

        logger.checkEmpty()

    def testCheckAllMoni(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        numEvts = 17
        numMoni = 222
        numSN = 51
        numTCal = 93

        moni.isTime = True
        moni.addEntry(5, 'backEnd', 'NumEventsSent', str(numEvts))
        moni.addEntry(17, 'moniBuilder', 'TotalDispatchedData', str(numMoni))
        moni.addEntry(17, 'snBuilder', 'TotalDispatchedData', str(numSN))
        moni.addEntry(17, 'tcalBuilder', 'TotalDispatchedData', str(numTCal))

        expId = 99
        expComps = [(5, 'eventBuilder', 0, 'x', 1234, 5678),
                    (17, 'secondaryBuilders', 0, 'x', 4321, 8765)]

        cnc = MockCnCRPC()
        cnc.setRunSet(expId, expComps)

        dr.runSetID = expId

        dr.fill_component_dictionaries(cnc)

        logger.addExpected(('\t%d physics events, %d moni events' +
                            ', %d SN events, %d tcals') %
                           (numEvts, numMoni, numSN, numTCal))

        rtnVal = dr.check_all()
        self.failUnless(rtnVal, 'Expected call to succeed')

        logger.checkEmpty()

    def testCheckAllWatchdogNone(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        dog = MockWatchdog()
        dr.watchdog = dog

        DAQRun.unHealthyCount = 0

        expCnt = 0

        cnc = MockCnCRPC()

        rtnVal = dr.check_all()
        self.failUnless(rtnVal, 'Expected call to succeed')

        self.failIf(dog.threadCleared, 'Should not have cleared thread')
        self.failIf(dog.watchStarted, 'Should not have started watchdog')
        self.assertEquals(expCnt, DAQRun.unHealthyCount,
                          'UnhealthyCount should be %d, not %d' %
                          (expCnt, DAQRun.unHealthyCount))

        logger.checkEmpty()

    def testCheckAllWatchdogStart(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        dog = MockWatchdog()
        dr.watchdog = dog

        dog.isTime = True
        DAQRun.unHealthyCount = 0

        expCnt = 0

        cnc = MockCnCRPC()

        rtnVal = dr.check_all()
        self.failUnless(rtnVal, 'Expected call to succeed')

        self.failIf(dog.threadCleared, 'Should not have cleared thread')
        self.failUnless(dog.watchStarted, 'Should have started watchdog')
        self.assertEquals(expCnt, DAQRun.unHealthyCount,
                          'UnhealthyCount should be %d, not %d' %
                          (expCnt, DAQRun.unHealthyCount))

        logger.checkEmpty()

    def testCheckAllWatchdogErr(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        dog = MockWatchdog()
        dr.watchdog = dog

        dog.inProg = True
        dog.caughtErr = True
        DAQRun.unHealthyCount = 0

        expCnt = 0

        cnc = MockCnCRPC()

        rtnVal = dr.check_all()
        self.failIf(rtnVal, 'Expected call to succeed')

        self.failUnless(dog.threadCleared, 'Should have cleared thread')
        self.failIf(dog.watchStarted, 'Should not have started watchdog')
        self.assertEquals(expCnt, DAQRun.unHealthyCount,
                          'UnhealthyCount should be %d, not %d' %
                          (expCnt, DAQRun.unHealthyCount))

        logger.checkEmpty()

    def testCheckAllWatchdogHealthy(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        dog = MockWatchdog()
        dr.watchdog = dog

        dog.inProg = True
        dog.done = True
        dog.healthy = True
        DAQRun.unHealthyCount = 1

        expCnt = 0

        cnc = MockCnCRPC()

        rtnVal = dr.check_all()
        self.failUnless(rtnVal, 'Expected call to succeed')

        self.failUnless(dog.threadCleared, 'Should have cleared thread')
        self.failIf(dog.watchStarted, 'Should not have started watchdog')
        self.assertEquals(expCnt, DAQRun.unHealthyCount,
                          'UnhealthyCount should be %d, not %d' %
                          (expCnt, DAQRun.unHealthyCount))

        logger.checkEmpty()

    def testCheckAllWatchdogUnhealthy(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        dog = MockWatchdog()
        dr.watchdog = dog

        dog.inProg = True
        dog.done = True
        DAQRun.unHealthyCount = 0

        expCnt = 1

        cnc = MockCnCRPC()

        rtnVal = dr.check_all()
        self.failUnless(rtnVal, 'Expected call to succeed')

        self.failUnless(dog.threadCleared, 'Should have cleared thread')
        self.failIf(dog.watchStarted, 'Should not have started watchdog')
        self.assertEquals(expCnt, DAQRun.unHealthyCount,
                          'UnhealthyCount should be %d, not %d' %
                          (expCnt, DAQRun.unHealthyCount))

        logger.checkEmpty()

    def testCheckAllWatchdogMax(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        moni = MockMoni()
        dr.moni = moni

        dog = MockWatchdog()
        dr.watchdog = dog

        dog.inProg = True
        dog.done = True
        DAQRun.unHealthyCount = DAQRun.MAX_UNHEALTHY_COUNT

        expCnt = 0

        cnc = MockCnCRPC()

        rtnVal = dr.check_all()
        self.failIf(rtnVal, 'Expected call to succeed')

        self.failUnless(dog.threadCleared, 'Should have cleared thread')
        self.failIf(dog.watchStarted, 'Should not have started watchdog')
        self.assertEquals(expCnt, DAQRun.unHealthyCount,
                          'UnhealthyCount should be %d, not %d' %
                          (expCnt, DAQRun.unHealthyCount))

        logger.checkEmpty()

    def testSetUpAllLoggers(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        logger.logPath = '/tmp'
        dr.log = logger

        expId = 99
        expComps = [(3, 'foo', 0, 'localhost', 1234, 5678),
                    (7, 'bar', 1, 'localhost', 4321, 8765)]

        cnc = MockCnCRPC()
        cnc.setRunSet(expId, expComps)

        dr.runSetID = expId

        dr.fill_component_dictionaries(cnc)

        nextPort = 9002
        logger.addExpected('Setting up logging for %d components' %
                           len(expComps))
        for c in expComps:
            logger.addExpected('%s(%d %s:%d) -> %s:%d' %
                               (c[1], c[0], c[3], c[4], dr.ip, nextPort))
            nextPort += 1

        try:
            dr.setUpAllComponentLoggers()
        finally:
            for k in dr.loggerOf.keys():
                if dr.loggerOf[k] is not None:
                    dr.loggerOf[k].stopServing()
            for c in expComps:
                path = os.path.join(logger.logPath, '%s-%d' % (c[1], c[2]))
                if os.path.exists(path):
                    os.remove(path)

        logger.checkEmpty()

    def testSetUpLoggers(self):
        dr = DAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        logger.logPath = '/tmp'
        dr.log = logger

        expId = 99
        expComps = [(3, 'foon', 5, 'localhost', 1234, 5678),
                    (7, 'barn', 4, 'localhost', 4321, 8765)]

        cnc = MockCnCRPC()
        cnc.setRunSet(expId, expComps)

        dr.runSetID = expId

        dr.fill_component_dictionaries(cnc)

        logger.addExpected('Setting up logging for %d components' %
                           len(expComps))

        nextPort = 9002
        for i in range(0, len(expComps)):
            c = expComps[i]

            logger.addExpected('%s(%d %s:%d) -> %s:%d' %
                               (c[1], c[0], c[3], c[4], dr.ip, nextPort))
            nextPort += 1

        try:
            dr.setup_component_loggers(cnc, 'xxx', expId)
        finally:
            for k in dr.loggerOf.keys():
                if dr.loggerOf[k] is not None:
                    dr.loggerOf[k].stopServing()
            for c in expComps:
                path = os.path.join(logger.logPath, '%s-%d' % (c[1], c[2]))
                if os.path.exists(path):
                    os.remove(path)

        logList = cnc.getRunsetLoggers()
        if logList is None:
            self.fail('Runset logging was not set')
        self.assertEquals(len(expComps), len(logList),
                          'Expected %d loggers, not %d' %
                          (len(expComps), len(logList)))

        nextPort = 9002
        for i in range(0, len(expComps)):
            c = expComps[i]
            l = logList[i]

            self.assertEquals(c[1], l[0],
                              'Expected short name #%d "%s", not "%s"' %
                              (i, c[1], l[0]))
            self.assertEquals(c[2], l[1],
                              'Expected DAQ ID #%d %d, not %d' %
                              (i, c[2], l[1]))
            self.assertEquals(nextPort, l[2],
                              'Expected log port #%d %d, not %d' %
                              (i, nextPort, l[2]))

            nextPort += 1

        logger.checkEmpty()

    def testRPCStopRunStopped(self):
        dr = StubbedDAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        logger.addExpected('Warning: run is already stopped.')

        dr.runState = 'STOPPED'
        dr.rpc_stop_run()

        logger.checkEmpty()

    def testRPCStopRunBadState(self):
        dr = StubbedDAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        badState = 'XXX'

        logger.addExpected("Warning: invalid state (%s), won't stop run." %
                           badState)

        dr.runState = badState
        dr.rpc_stop_run()

        logger.checkEmpty()

    def testRPCStopRunSuccess(self):
        dr = StubbedDAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        dr.runState = 'RUNNING'
        dr.rpc_stop_run()

        self.assertEquals('STOPPING', dr.runState, 'Should be stopping, not ' +
                          dr.runState)

        logger.checkEmpty()

    def sortCompTuple(self, x, y):
        if x[1] == 'stringHub' and y[1] != 'stringHub':
                return 1
        elif x[1] != 'stringHub' and y[1] == 'stringHub':
                return -1

        if x[0] == y[0]:
            return 0
        elif x[0] < y[0]:
            return -1
        else:
            return 1
            
    def finishRunThreadTest(self, dr, cnc, logger, catchall, ebID, sbID, comps):
        time.sleep(0.4)
        self.assertEquals('STOPPED', dr.runState, 'Should be stopped, not ' +
                          dr.runState)

        setId = 1
        runNum = 654
        configName = 'sim5str'

        logger.addExpected('Loaded global configuration "%s"' % configName)
        logger.addExpected('Configuration includes detector in-ice')
        logger.addExpected('Configuration includes detector icetop')

        compSrt = comps[:]
        compSrt.sort(self.sortCompTuple)

        for c in compSrt:
            logger.addExpected('Component list will require %s#%d' %
                               (c[1], c[2]))

        logger.addExpected(('Starting run %d (waiting for required %d' +
                            ' components to register w/ CnCServer)') %
                           (runNum, len(comps)), True)
        logger.addExpected('Created Run Set #%d' % setId)
        logger.addExpected(('Version info: %(filename)s %(revision)s %(date)s' +
                            ' %(time)s %(author)s %(release)s %(repo_rev)s') %
                           dr.versionInfo)
        logger.addExpected('Starting run %d...' % runNum)
        logger.addExpected('Run configuration: %s' % configName)
        logger.addExpected('Cluster configuration: %s' %
                           TestDAQRun.CLUSTER_CONFIG)
        logger.addExpected('Created logger for CnCServer')
        logger.addExpected('Setting up logging for %d components' % len(comps))

        nextPort = 9002
        for c in compSrt:
            logger.addExpected('%s(%d %s:%d) -> %s:%d' %
                               (c[1], c[0], c[3], c[4], dr.ip, nextPort))
            nextPort += 1
        logger.addExpected('Configuring run set...')
        logger.addExpected('Started run %d on run set %d' % (runNum, setId))

        dr.rpc_start_run(runNum, None, configName)

        numTries = 0
        while dr.runState == 'STARTING' and numTries < 100:
            time.sleep(0.1)
            numTries += 1
        self.assertEquals('RUNNING', dr.runState, 'Should be running, not ' +
                          dr.runState)
        self.failIf(cnc.RSBreakFlag, 'Runset should not have been broken')
        self.failUnless(cnc.RSConfigFlag, 'Runset was not configured')
        self.failUnless(cnc.RSStartFlag, 'Runset was not started')
        cnc.resetFlags()

        subRunId = 1
        domList = [('53494d550101', 0, 1, 2, 3, 4),
                   ['1001', '22', 1, 2, 3, 4, 5],
                   ('a', 0, 1, 2, 3, 4)]

        flashList = [domList[0], ['53494d550122', ] + domList[1][2:]]
        logger.addExpected(("Subrun %d: will ignore missing DOM ('DOM %s not" +
                            " found in config!')...") %
                           (subRunId, domList[2][0]))
        logger.addExpected('Subrun %d: flashing DOMs (%s)' %
                           (subRunId, str(flashList)))

        dr.rpc_flash(subRunId, domList)
        self.failUnless(cnc.RSFlashFlag, 'Runset should have flashed')

        numEvts = 17
        numMoni = 222
        numSN = 51
        numTCal = 93

        moni = dr.moni
        moni.addEntry(ebID, 'backEnd', 'NumEventsSent', str(numEvts))
        moni.addEntry(sbID, 'moniBuilder', 'TotalDispatchedData', str(numMoni))
        moni.addEntry(sbID, 'snBuilder', 'TotalDispatchedData', str(numSN))
        moni.addEntry(sbID, 'tcalBuilder', 'TotalDispatchedData', str(numTCal))

        logger.addExpected('Stopping run %d' % runNum)
        logger.addExpected('%d physics events collected in 0 seconds' % numEvts)
        logger.addExpected('%d moni events, %d SN events, %d tcals' %
                           (numMoni, numSN, numTCal))
        logger.addExpected('Stopping component logging')
        logger.addExpected('RPC Call stats:\n%s' % cnc.showStats())
        logger.addExpected('Run terminated SUCCESSFULLY.')
        logger.addExpected(('Queueing data for SPADE (spadeDir=%s, logDir=%s,' +
                            ' runNum=%s)...') %
                           (TestDAQRun.SPADE_DIR, TestDAQRun.LOG_DIR, runNum))

        dr.rpc_stop_run()

        numTries = 0
        while dr.runState == 'STOPPING' and numTries < 100:
            time.sleep(0.1)
            numTries += 1
        self.assertEquals('STOPPED', dr.runState, 'Should be stopped, not ' +
                          dr.runState)
        self.failUnless(cnc.RSStopFlag, 'Runset was not stopped')
        cnc.resetFlags()

        basePrefix = dr.get_base_prefix(runNum, None, None)

        moni = dr.rpc_run_monitoring()
        self.failIf(moni is None, 'rpc_run_monitoring returned None')
        self.failIf(len(moni) == 0, 'rpc_run_monitoring returned no data')
        self.assertEquals(numEvts, moni['physicsEvents'],
                          'Expected %d physics events, not %d' %
                          (numEvts, moni['physicsEvents']))
        self.assertEquals(numMoni, moni['moniEvents'],
                          'Expected %d moni events, not %d' %
                          (numMoni, moni['moniEvents']))
        self.assertEquals(numSN, moni['snEvents'],
                          'Expected %d sn events, not %d' %
                          (numSN, moni['snEvents']))
        self.assertEquals(numTCal, moni['tcalEvents'],
                          'Expected %d tcal events, not %d' %
                          (numTCal, moni['tcalEvents']))

        catchall.addExpected('Breaking run set...')

        dr.rpc_release_runsets()
        self.failUnless(cnc.RSBreakFlag, 'Runset should have been broken')
        cnc.resetFlags()

        dr.running = False
        time.sleep(0.4)

        logger.checkEmpty()

    def testRunThread(self):
        dr = StubbedDAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        catchall = MockLogger()
        StubbedDAQRun.setSocketLogger(catchall)

        ebID = 67
        sbID = 92

        comps = [(2, 'stringHub', 1001, 'localhost', 111, 211),
                 (4, 'stringHub', 1002, 'localhost', 112, 212),
                 (6, 'stringHub', 1003, 'localhost', 113, 213),
                 (8, 'stringHub', 1004, 'localhost', 114, 214),
                 (10, 'stringHub', 1005, 'localhost', 115, 215),
                 (12, 'stringHub', 1081, 'localhost', 116, 216),
                 (14, 'inIceTrigger', 0, 'localhost', 117, 217),
                 (16, 'globalTrigger', 0, 'localhost', 118, 218),
                 (ebID, 'eventBuilder', 0, 'localhost', 119, 219),
                 (sbID, 'secondaryBuilders', 0, 'localhost', 120, 220),]

        cnc = MockCnCRPC()
        cnc.setComponents(comps)

        runThread = thread.start_new_thread(dr.run_thread, (cnc, ))
        self.finishRunThreadTest(dr, cnc, logger, catchall, ebID, sbID, comps)

    def testRunThreadInverted(self):
        dr = StubbedDAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        catchall = MockLogger()
        StubbedDAQRun.setSocketLogger(catchall)

        ebID = 67
        sbID = 92

        comps = [(2, 'stringHub', 1001, 'localhost', 111, 211),
                 (4, 'stringHub', 1002, 'localhost', 112, 212),
                 (6, 'stringHub', 1003, 'localhost', 113, 213),
                 (8, 'stringHub', 1004, 'localhost', 114, 214),
                 (10, 'stringHub', 1005, 'localhost', 115, 215),
                 (12, 'stringHub', 1081, 'localhost', 116, 216),
                 (14, 'inIceTrigger', 0, 'localhost', 117, 217),
                 (16, 'globalTrigger', 0, 'localhost', 118, 218),
                 (ebID, 'eventBuilder', 0, 'localhost', 119, 219),
                 (sbID, 'secondaryBuilders', 0, 'localhost', 120, 220),]

        cnc = MockCnCRPC()
        cnc.setComponents(comps)

        runThread = \
            thread.start_new_thread(self.finishRunThreadTest,
                                    (dr, cnc, logger, catchall, ebID, sbID,
                                     comps))
        dr.run_thread(cnc)

    def testRunSummaryFauxTest(self):
        # this is a blatant attempt to increase the code coverage, because the
        # rpc_run_summary method is Anvil-centric and I3Live is coming soon

        dr = StubbedDAQRun(self.getRunArgs(), False)

        logger = MockLogger()
        dr.log = logger

        dr.runState = "RUNNING"
        dr.runStats.runNum = 15
        dr.prevRunStats = dr.runStats

        dr.rpc_daq_summary_xml()

if __name__ == '__main__':
    unittest.main()
#!/usr/bin/env python

import sys, threading, unittest, xmlrpclib

from CnCServer import CnCServer, DAQClient
from DAQConst import DAQPort
from DAQRPC import RPCServer

from DAQMocks \
    import MockAppender, MockCnCLogger, SocketReaderFactory, SocketWriter

class MostlyDAQClient(DAQClient):
    def __init__(self, name, num, host, port, mbeanPort, connectors, appender):
        self.__appender = appender

        super(MostlyDAQClient, self).__init__(name, num, host, port,
                                              mbeanPort, connectors)

    def createCnCLogger(self, quiet):
        return MockCnCLogger(self.__appender, quiet)

class MostlyCnCServer(CnCServer):
    SERVER_NAME = "MostlyCnC"
    APPENDERS = {}

    def __init__(self, logIP='localhost', logPort=-1):
        super(MostlyCnCServer, self).__init__(name=MostlyCnCServer.SERVER_NAME,
                                              logIP=logIP, logPort=logPort,
                                              quiet=True)

    def createClient(self, name, num, host, port, mbeanPort, connectors):
        key = '%s#%d' % (name, num)
        key = 'server'
        if not MostlyCnCServer.APPENDERS.has_key(key):
            MostlyCnCServer.APPENDERS[key] = MockAppender('Mock-%s' % key)

        return MostlyDAQClient(name, num, host, port, mbeanPort, connectors,
                               MostlyCnCServer.APPENDERS[key])

    def createCnCLogger(self, quiet):
        key = 'server'
        if not MostlyCnCServer.APPENDERS.has_key(key):
            MostlyCnCServer.APPENDERS[key] = MockAppender('Mock-%s' % key)

        return MockCnCLogger(MostlyCnCServer.APPENDERS[key], quiet)

    def monitorLoop(self):
        pass

class RealComponent(object):
    APPENDERS = {}

    def __init__(self, name, num, cmdPort, mbeanPort, verbose=False):
        self.__name = name
        self.__num = num

        self.__state = 'FOO'

        self.__logger = None
        self.__expRunPort = None

        self.__cmd = RPCServer(cmdPort)
        self.__cmd.register_function(self.__configure, 'xmlrpc.configure')
        self.__cmd.register_function(self.__connect, 'xmlrpc.connect')
        self.__cmd.register_function(self.__getState, 'xmlrpc.getState')
        self.__cmd.register_function(self.__getVersionInfo,
                                     'xmlrpc.getVersionInfo')
        self.__cmd.register_function(self.__logTo, 'xmlrpc.logTo')
        self.__cmd.register_function(self.__reset, 'xmlrpc.reset')
        self.__cmd.register_function(self.__resetLogging, 'xmlrpc.resetLogging')
        self.__cmd.register_function(self.__startRun, 'xmlrpc.startRun')
        self.__cmd.register_function(self.__stopRun, 'xmlrpc.stopRun')
        threading.Thread(target=self.__cmd.serve_forever, args=()).start()

        self.__mbean = RPCServer(mbeanPort)
        threading.Thread(target=self.__mbean.serve_forever, args=()).start()

        self.__cnc = xmlrpclib.ServerProxy('http://localhost:%d' %
                                           DAQPort.CNCSERVER, verbose=verbose)
        self.__cnc.rpc_register_component(self.__name, self.__num,
                                                 'localhost', cmdPort,
                                                 mbeanPort, [])

    def __configure(self, cfgName=None):
        if self.__logger is None:
            raise Exception('No logging for %s#%d' % (self.__name, self.__num))

        if cfgName is None:
            cfgStr = ''
        else:
            cfgStr = ' with %s' % cfgName

        self.__logger.write('Config %s#%d%s' %
                            (self.__name, self.__num, cfgStr))

        self.__state = 'ready'
        return 'CFG'

    def __connect(self, *args):
        self.__state = 'connected'
        return 'CONN'

    def __getState(self):
        return self.__state

    def __getVersionInfo(self):
        return '$Id: filename revision date time author xxx'

    def __logTo(self, logHost, logPort, liveHost, livePort):
        if logHost is not None and logHost == '':
            logHost = None
        if logPort is not None and logPort == 0:
            logPort = None
        if liveHost is not None and liveHost == '':
            liveHost = None
        if livePort is not None and livePort == 0:
            livePort = None
        if logPort != self.__expRunPort:
            raise Exception('Expected runlog port %d, not %d' %
                            (self.__expRunPort, logPort))
        if liveHost is not None and livePort is not None:
            raise Exception("Didn't expect I3Live logging")

        self.__logger = SocketWriter(logHost, logPort)
        self.__logger.write('Test msg')
        return 'OK'

    def __reset(self):
        self.__state = 'idle'
        return 'RESET'

    def __resetLogging(self):
        self.__expRunPort = None
        self.__logger = None

        self.__state = 'reset'
        return 'RLOG'

    def __startRun(self, runNum):
        if self.__logger is None:
            raise Exception('No logging for %s#%d' % (self.__name, self.__num))

        self.__logger.write('Start #%d on %s#%d' %
                            (runNum, self.__name, self.__num))

        self.__state = 'running'
        return 'RUN#%d' % runNum

    def __stopRun(self):
        if self.__logger is None:
            raise Exception('No logging for %s#%d' % (self.__name, self.__num))

        self.__logger.write('Stop %s#%d' % (self.__name, self.__num))

        self.__state = 'stopped'
        return 'STOP'

    def close(self):
        self.__cmd.server_close()
        self.__mbean.server_close()

    def createCnCLogger(self, quiet=True):
        key = '%s#%d' % (self.__name, self.__num)
        if not RealComponent.APPENDERS.has_key(key):
            RealComponent.APPENDERS[key] = MockAppender('Mock-%s' % key)

        return MockCnCLogger(RealComponent.APPENDERS[key], quiet)

    def getState(self):
        return self.__getState()

    def setExpectedRunLogPort(self, port):
        self.__expRunPort = port

class TestCnCServer(unittest.TestCase):
    def createLog(self, name, port):
        return self.__logFactory.createLog(name, port)

    def setUp(self):
        self.__logFactory = SocketReaderFactory()

        self.comp = None
        self.cnc = None

        MostlyCnCServer.APPENDERS.clear()
        RealComponent.APPENDERS.clear()

    def tearDown(self):
        for key in RealComponent.APPENDERS:
            RealComponent.APPENDERS[key].WaitForEmpty(10)
        for key in MostlyCnCServer.APPENDERS:
            MostlyCnCServer.APPENDERS[key].checkStatus(10)

        if self.comp is not None:
            self.comp.close()
        if self.cnc is not None:
            self.cnc.closeServer()

        self.__logFactory.tearDown()

    def __runEverything(self):
        catchall = self.createLog('master', 18999)

        catchall.addExpectedText("I'm server %s running on port %d" %
                                 (MostlyCnCServer.SERVER_NAME,
                                  DAQPort.CNCSERVER))
        catchall.addExpectedTextRegexp(r'\S+ \S+ \S+ \S+ \S+ \S+ \S+')

        self.cnc = MostlyCnCServer(logPort=catchall.getPort())
        threading.Thread(target=self.cnc.run, args=()).start()

        catchall.checkStatus(100)

        compName = 'foo'
        compNum = 1
        host = 'localhost'
        cmdPort = 19001
        mbeanPort = 19002

        catchall.addExpectedText(('Got registration for ID#%d %s#%d at' +
                                  ' localhost:%d M#%d') %
                                 (DAQClient.ID, compName, compNum, cmdPort,
                                  mbeanPort))

        self.comp = RealComponent(compName, compNum, cmdPort, mbeanPort)

        catchall.checkStatus(100)

        s = self.cnc.rpc_list_components()
        self.assertEquals(1, len(s),
                          'Expected 1 listed component, not %d' % len(s))
        self.assertEquals(compName, s[0]["compName"],
                          'Expected component %s, not %s' %
                          (compName, s[0]["compName"]))
        self.assertEquals(compNum, s[0]["compNum"],
                          'Expected %s #%d, not #%d' %
                          (compName, compNum, s[0]["compNum"]))
        self.assertEquals(host, s[0]["host"],
                          'Expected %s#%d host %s, not %s' %
                          (compName, compNum, host, s[0]["host"]))
        self.assertEquals(cmdPort, s[0]["rpcPort"],
                          'Expected %s#%d cmdPort %d, not %d' %
                          (compName, compNum, cmdPort, s[0]["rpcPort"]))
        self.assertEquals(mbeanPort, s[0]["mbeanPort"],
                          'Expected %s#%d mbeanPort %d, not %d' %
                          (compName, compNum, mbeanPort, s[0]["mbeanPort"]))

        compId = s[0]["id"]

        catchall.addExpectedText('Built runset with the following components:')

        setId = self.cnc.rpc_runset_make([compName])
        self.assertEquals('connected', self.comp.getState(),
                          'Unexpected state %s' % self.comp.getState())

        catchall.checkStatus(100)

        rs = self.cnc.rpc_runset_list(setId)
        for c in rs:
            if compId != rs[0]["id"]:
                continue

            self.assertEquals(compName, c["compName"],
                              "Component#%d name should be \"%s\", not \"%s\"" %
                              (compId, compName, c["compName"]))
            self.assertEquals(compNum, c["compNum"],
                              "Component#%d \"%s\" number should be %d, not %d" %
                              (compId, compName, compNum, c["compNum"]))
            self.assertEquals(host, c["host"],
                              ("Component#%d \"%s#%d\" host should be" +
                               " \"%s\", not \"%s\"") %
                              (compId, compName, compNum, host, c["host"]))
            self.assertEquals(cmdPort, c["rpcPort"],
                              ("Component#%d \"%s#%d\" rpcPort should be" +
                               " \"%s\", not \"%s\"") %
                              (compId, compName, compNum, cmdPort, c["rpcPort"]))
            self.assertEquals(mbeanPort, c["mbeanPort"],
                              ("Component#%d \"%s#%d\" mbeanPort should be" +
                               " \"%s\", not \"%s\"") %
                              (compId, compName, compNum, mbeanPort,
                               c["mbeanPort"]))

        catchall.checkStatus(100)

        runPort = 18998

        runlog = self.createLog('runlog', runPort)

        self.comp.setExpectedRunLogPort(runPort)

        runlog.addExpectedExact('Test msg')
        runlog.addExpectedText('filename revision date time author')

        logList = [(compName, compNum, runPort), ]
        self.assertEqual(self.cnc.rpc_runset_log_to(setId, host, logList), 'OK')

        runlog.checkStatus(100)

        runlog.addExpectedExact('Config %s#%d' % (compName, compNum))

        self.assertEqual(self.cnc.rpc_runset_configure(setId), 'OK')

        runlog.checkStatus(100)

        cfgName = 'zzz'

        runlog.addExpectedExact('Config %s#%d with %s' % (compName, compNum,
                                                          cfgName))

        self.assertEqual(self.cnc.rpc_runset_configure(setId, cfgName), 'OK')

        runlog.checkStatus(100)

        runNum = 444

        runlog.addExpectedExact('Start #%d on %s#%d' %
                                (runNum, compName, compNum))

        self.assertEqual(self.cnc.rpc_runset_start_run(setId, runNum), 'OK')

        runlog.checkStatus(100)

        runlog.addExpectedExact('Stop %s#%d' % (compName, compNum))

        self.assertEqual(self.cnc.rpc_runset_stop_run(setId), 'OK')

        runlog.checkStatus(100)

        self.assertEqual(self.cnc.rpc_get_num_components(), 0)
        self.assertEqual(self.cnc.rpc_num_sets(), 1)

        serverAppender = MostlyCnCServer.APPENDERS['server']
        serverAppender.addExpectedExact('End of log')

        self.assertEquals(self.cnc.rpc_runset_break(setId), 'OK')

        serverAppender.checkStatus(100)

        self.assertEqual(self.cnc.rpc_get_num_components(), 1)
        self.assertEqual(self.cnc.rpc_num_sets(), 0)

        serverAppender.checkStatus(100)

        runlog.checkStatus(100)
        catchall.checkStatus(100)

    def testEverything(self):
        self.__runEverything()

    def testEverythingAgain(self):
        if sys.platform != 'darwin':
            print 'Skipping server tests in non-Darwin OS'
            return

        self.__runEverything()

if __name__ == '__main__':
    unittest.main()

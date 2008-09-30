#!/usr/bin/env python

import unittest
from CnCServer import DAQClient, DAQServer
from DAQLog import SocketLogger

class MockXMLRPC:
    def __init__(self):
        pass

    def configure(self, name=None):
        pass

    def connect(self, name=None):
        return 'OK'

    def getState(self):
        pass

    def getVersionInfo(self):
        return ''

    def logTo(self, logIP, port):
        pass

    def reset(self):
        pass

    def resetLogging(self):
        pass

    def startRun(self, runNum):
        pass

    def stopRun(self):
        pass

class MockRPCClient:
    def __init__(self, host, port):
        self.xmlrpc = MockXMLRPC()

class MockLogger(object):
    def __init__(self, host, port):
        pass

    def write_ts(self, s):
        pass

class MockClient(DAQClient):
    def __init__(self, name, num, host, port, mbeanPort, connectors):

        self.state = 'idle'

        super(MockClient, self).__init__(name, num, host, port, mbeanPort,
                                         connectors)

    def configure(self, cfgName):
        self.state = 'ready'
        return super(MockClient, self).configure(cfgName)

    def connect(self, connMap=None):
        self.state = 'connected'
        return super(MockClient, self).connect(connMap)

    def createClient(self, host, port):
        return MockRPCClient(host, port)

    def createLogger(self, host, port):
        return MockLogger(host, port)

    def getState(self):
        return self.state

    def reset(self):
        self.state = 'idle'
        return super(MockClient, self).reset()

    def startRun(self, runNum):
        self.state = 'running'
        return super(MockClient, self).startRun(runNum)

class MockServer(DAQServer):
    def __init__(self):
        super(MockServer, self).__init__(testOnly=True)

    def createClient(self, name, num, host, port, mbeanPort, connectors):
        return MockClient(name, num, host, port, mbeanPort, connectors)

class TestDAQServer(unittest.TestCase):
    def testRegister(self):
        dc = MockServer()

        self.assertEqual(dc.rpc_show_components(), [])

        compName = 'foo'
        compNum = 0
        compHost = 'localhost'
        compPort = 666
        compMBean = 0
        rtnArray = dc.rpc_register_component(compName, compNum, compHost,
                                             compPort, 0, [])

        self.assertEqual(dc.rpc_get_num_components(), 1)

        fooStr = 'ID#' + str(DAQClient.ID - 1) + ' ' + compName + '#' + \
            str(compNum) + ' at ' + compHost + ':' + str(compPort) + ' ' + \
            'idle'
        self.assertEqual(dc.rpc_show_components(), [fooStr])

        self.assertEqual(len(rtnArray), 4)
        self.assertEqual(rtnArray[0], DAQClient.ID - 1)
        self.assertEqual(rtnArray[1], '')
        self.assertEqual(rtnArray[2], 0)

    def testRegisterWithLog(self):
        dc = MockServer()

        logHost = 'localhost'
        logPort = 123

        self.failUnless(dc.socketlog is None, 'socketlog is None')

        dc.rpc_log_to(logHost, logPort)
        self.failIf(dc.socketlog is None, 'socketlog is None')
        self.assertEqual(dc.logIP, logHost)
        self.assertEqual(dc.logPort, logPort)

        rtnArray = dc.rpc_register_component('foo', 0, 'localhost', 666, 0, [])

        self.assertEqual(len(rtnArray), 4)
        self.assertEqual(rtnArray[0], DAQClient.ID - 1)
        self.assertEqual(rtnArray[1], logHost)
        self.assertEqual(rtnArray[2], logPort)

        dc.rpc_close_log()
        self.failIf(dc.socketlog is not None, 'socketlog is not None')
        self.assertEqual(dc.logIP, None)
        self.assertEqual(dc.logPort, None)

    def testLogFallback(self):
        dc = MockServer()

        logHost = 'localhost'
        logPort = 12345

        logObj = SocketLogger(logPort, 'log', None)
        logObj.startServing()

        try:
            self.failIf(dc.socketlog is not None, 'socketlog is not None')

            dc.rpc_log_to(logHost, logPort)
            self.failIf(dc.socketlog is None, 'socketlog is None')
            self.assertEqual(dc.logIP, logHost)
            self.assertEqual(dc.logPort, logPort)
            self.assertEqual(dc.prevIP, logHost)
            self.assertEqual(dc.prevPort, logPort)
            dc.rpc_close_log()

            self.failIf(dc.socketlog is not None, 'socketlog is not None')
            self.failIf(dc.logIP is not None, 'logIP is not None')
            self.failIf(dc.logPort is not None, 'logPort is not None')
            self.assertEqual(dc.prevIP, logHost)
            self.assertEqual(dc.prevPort, logPort)

            newHost = 'localhost'
            newPort = 456778

            newObj = SocketLogger(newPort, 'new', None)
            newObj.startServing()

            try:
                dc.rpc_log_to(newHost, newPort)
                self.failIf(dc.socketlog is None, 'socketlog is None')
                self.assertEqual(dc.logIP, newHost)
                self.assertEqual(dc.logPort, newPort)
                self.assertEqual(dc.prevIP, logHost)
                self.assertEqual(dc.prevPort, logPort)

                dc.rpc_close_log()
                self.failIf(dc.socketlog is None, 'socketlog is None')
                self.assertEqual(dc.logIP, logHost)
                self.assertEqual(dc.logPort, logPort)
                self.assertEqual(dc.prevIP, logHost)
                self.assertEqual(dc.prevPort, logPort)
            finally:
                newObj.stopServing()

            dc.rpc_close_log()
            self.failIf(dc.socketlog is not None, 'socketlog is not None')
            self.assertEqual(dc.logIP, None)
            self.assertEqual(dc.logPort, None)
        finally:
            logObj.stopServing()

    def testNoRunset(self):
        dc = MockServer()

        self.assertRaises(ValueError, dc.rpc_runset_break, 1)
        self.assertRaises(ValueError, dc.rpc_runset_configure, 1)
        self.assertRaises(ValueError, dc.rpc_runset_configure, 1, 'xxx')
        self.assertRaises(ValueError, dc.rpc_runset_log_to, 1, 'xxx', [])
        self.assertRaises(ValueError, dc.rpc_runset_start_run, 1, 1)
        self.assertRaises(ValueError, dc.rpc_runset_status, 1)
        self.assertRaises(ValueError, dc.rpc_runset_stop_run, 1)

    def testRunset(self):
        dc = MockServer()

        self.assertEqual(dc.rpc_get_num_components(), 0)
        self.assertEqual(dc.rpc_num_sets(), 0)
        self.assertEqual(dc.rpc_show_components(), [])

        rtnArray = dc.rpc_register_component('foo', 0, 'localhost', 666, 0, [])

        self.assertEqual(dc.rpc_get_num_components(), 1)
        self.assertEqual(dc.rpc_num_sets(), 0)

        setId = dc.rpc_runset_make(['foo'])

        self.assertEqual(dc.rpc_get_num_components(), 0)
        self.assertEqual(dc.rpc_num_sets(), 1)

        self.assertEqual(dc.rpc_runset_status(setId), 'OK')

        self.assertEqual(dc.rpc_runset_log_to(setId, 'abc',
                                              [['foo', 0, 777, 'fatal'], ]),
                         'OK')

        self.assertEqual(dc.rpc_runset_configure(setId), 'OK')

        self.assertEqual(dc.rpc_runset_configure(setId, 'zzz'), 'OK')

        self.assertEqual(dc.rpc_runset_start_run(setId, 444), 'OK')

        self.assertEqual(dc.rpc_runset_stop_run(setId), 'OK')

        self.assertEqual(dc.rpc_get_num_components(), 0)
        self.assertEqual(dc.rpc_num_sets(), 1)

        self.assertEquals(dc.rpc_runset_break(setId), 'OK')

        self.assertEqual(dc.rpc_get_num_components(), 1)
        self.assertEqual(dc.rpc_num_sets(), 0)

if __name__ == '__main__':
    unittest.main()

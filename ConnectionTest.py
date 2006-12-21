#!/usr/bin/env python

import unittest
from CnCServer import Connector, DAQClient, DAQPool

LOUD = False

class Node(object):
    IS_OUTPUT = True
    IS_INPUT = False

    CONN_PORT = -1

    def __init__(self, name, num=0):
        self.name = name
        self.num = num
        self.outLinks = {}
        self.inLinks = {}

    def __str__(self):
        return self.name + '#' + str(self.num)

    def connectOutputTo(self, comp, ioType):
        self.link(comp, ioType, Node.IS_OUTPUT)
        comp.link(self, ioType, Node.IS_INPUT)

    def getConnections(self):
        connectors = []
        for k in self.outLinks.keys():
            connectors.append(Connector(k, False, self.getNextPort()))
        for k in self.inLinks.keys():
            connectors.append(Connector(k, True, self.getNextPort()))
        return connectors

    def getDescription(self):
        rtnStr = str(self)

        if len(self.outLinks) > 0:
            rtnStr += ' OUT['

            firstK = True
            for k in self.outLinks.keys():
                if firstK:
                    firstK = False
                else:
                    rtnStr += ','

                rtnStr += k + '='

                firstL = True
                for l in self.outLinks[k]:
                    if firstL:
                        firstL = False
                    else:
                        rtnStr += ','

                    rtnStr += str(l)

            rtnStr += ']'
                
        if len(self.inLinks) > 0:
            rtnStr += ' IN['

            firstK = True
            for k in self.inLinks.keys():
                if firstK:
                    firstK = False
                else:
                    rtnStr += ','

                rtnStr += k + '='

                firstL = True
                for l in self.inLinks[k]:
                    if firstL:
                        firstL = False
                    else:
                        rtnStr += ','

                    rtnStr += str(l)

            rtnStr += ']'

        return rtnStr

    def getNextPort(self):
        port = Node.CONN_PORT
        Node.CONN_PORT -= 1
        return port

    def link(self, comp, ioType, isOutput):
        if isOutput:
            map = self.outLinks
        else:
            map = self.inLinks

        if not map.has_key(ioType):
            map[ioType] = []

        map[ioType].append(comp)

class MockXMLRPC:
    def __init__(self, name, num, outLinks):
        self.name = name
        self.num = num

        self.outLinks = outLinks

    def configure(self, id, name=None):
        pass

    def connect(self, id, list=None):
        if not list:
            return 'OK'

        if LOUD:
            print 'Conn[' + self.name + ':' + str(self.num) + ']'
            for l in list:
                print '  ' + l.type + ':' + l.compName + '#' + str(l.compNum)

        # make a copy of the links
        #
        tmpLinks = {}
        for k in self.outLinks.keys():
            tmpLinks[k] = []
            tmpLinks[k][0:] = self.outLinks[k][0:len(self.outLinks[k])]

        for l in list:
            if not tmpLinks.has_key(l.type):
                raise ValueError, 'Component ' + self.name + '#' + \
                    str(self.num) + ' should not have a "' + l.type + \
                    '" connection'

            comp = None
            for t in tmpLinks[l.type]:
                if t.name == l.compName and t.num == l.compNum:
                    comp = t
                    tmpLinks[l.type].remove(t)
                    if len(tmpLinks[l.type]) == 0:
                        del tmpLinks[l.type]
                    break

            if not comp:
                raise ValueError, 'Component ' + self.name + '#' + \
                    str(self.num) + ' should not connect to ' + \
                    l.type + ':' + l.compName + '#' + str(l.compNum)

        if len(tmpLinks) > 0:
            errMsg = 'Component ' + self.name + '#' + str(self.num) + \
                ' is not connected to '

            first = True
            for k in tmpLinks.keys():
                for t in tmpLinks[k]:
                    if first:
                        first = False
                    else:
                        errMsg += ', '
                    errMsg += k + ':' + t.name + '#' + str(t.num)
            raise ValueError, errMsg

        return 'OK'

    def getState(self, id):
        pass

    def logTo(self, id, logIP, port, level):
        pass

    def reset(self, id):
        pass

    def startRun(self, id, runNum):
        pass

    def stopRun(self, id):
        pass

class MockRPCClient:
    def __init__(self, name, num, outLinks):
        self.xmlrpc = MockXMLRPC(name, num, outLinks)

class MockLogger(object):
    def __init__(self, host, port):
        pass

    def write_ts(self, s):
        pass

class MockClient(DAQClient):
    def __init__(self, name, num, host, port, connectors, outLinks):

        self.outLinks = outLinks

        super(MockClient, self).__init__(name, num, host, port, connectors)

    def createClient(self, host, port):
        return MockRPCClient(self.name, self.num, self.outLinks)

    def createLogger(self, host, port):
        return MockLogger(self.name, self.num)

class ConnectionTest(unittest.TestCase):
    EXP_ID = 1

    def buildRunset(self, nodeList):
        if LOUD:
            print '-- Nodes'
            for node in nodeList:
                print node.getDescription()

        pool = DAQPool()
        port = -1
        for node in nodeList:
            pool.add(MockClient(node.name, node.num, None, port,
                                node.getConnections(), node.outLinks))
            port -= 1

        if LOUD:
            print '-- Pool has ' + str(len(pool.pool)) + ' comps'
            for k in pool.pool.keys():
                print '  ' + str(k)
                for c in pool.pool[k]:
                    print '    ' + str(c)

        numComps = len(pool.pool)

        nameList = []
        for node in nodeList:
            nameList.append(node.name + '#' + str(node.num))

        set = pool.makeSet(nameList)

        chkId = ConnectionTest.EXP_ID
        ConnectionTest.EXP_ID += 1

        self.assertEquals(len(pool.pool), 0)
        self.assertEquals(len(pool.sets), 1)
        self.assertEquals(pool.sets[0], set)

        self.assertEquals(set.id, chkId)
        self.assertEquals(len(set.set), len(nodeList))

        # copy node list
        #
        tmpList = []
        tmpList[0:] = nodeList[0:len(nodeList)]

        # validate all components in runset
        #
        for comp in set.set:
            node = None
            for t in tmpList:
                if comp.name == t.name and comp.num == t.num:
                    node = t
                    tmpList.remove(t)
                    break

            self.failIf(not node, 'Could not find component ' + str(comp))

            # copy connector list
            #
            compConn = []
            compConn[0:] = comp.connectors[0:len(comp.connectors)]

            # remove all output connectors
            #
            for type in node.outLinks:
                conn = None
                for c in compConn:
                    if not c.isInput and c.type == type:
                        conn = c
                        compConn.remove(c)
                        break

                self.failIf(not conn, 'Could not find connector ' + type +
                            ' for component ' + str(comp))

            # remove all input connectors
            #
            for type in node.inLinks:
                conn = None
                for c in compConn:
                    if c.isInput and c.type == type:
                        conn = c
                        compConn.remove(c)
                        break

                self.failIf(not conn, 'Could not find connector ' + type +
                            ' for component ' + str(comp))

            # whine if any connectors are left
            #
            self.assertEquals(len(compConn), 0, 'Found extra connectors in ' +
                              str(compConn))

        # whine if any components are left
        #
        self.assertEquals(len(tmpList), 0, 'Found extra components in ' +
                          str(tmpList))

        if LOUD:
            print '-- SET: ' + str(set)

        pool.returnSet(set)
        self.assertEquals(len(pool.pool), numComps)
        self.assertEquals(len(pool.sets), 0)

    def testSimple(self):
        # build nodes
        #
        n1 = Node('one')
        n2 = Node('two')
        n3 = Node('three')
        n4 = Node('four')

        # connect nodes
        #
        n1.connectOutputTo(n2, 'out1')
        n2.connectOutputTo(n3, 'out2')
        n3.connectOutputTo(n4, 'out3')

        # build list of all nodes
        #
        allNodes = [n1, n2, n3, n4]

        self.buildRunset(allNodes)

    def testStandard(self):
        # build nodes
        #
        shList = []
        ihList = []

        for i in range(0,4):
            shList.append(Node('StringHub', i + 10))
            ihList.append(Node('IcetopHub', i + 20))

        gt = Node('GlobalTrigger')
        iit = Node('InIceTrigger')
        itt = Node('IceTopTrigger')
        eb = Node('EventBuilder')

        # connect nodes
        #
        for sh in shList:
            sh.connectOutputTo(iit, 'stringHit')
            eb.connectOutputTo(sh, 'rdoutReq')
            sh.connectOutputTo(eb, 'rdoutData')

        for ih in ihList:
            ih.connectOutputTo(itt, 'icetopHit')
            eb.connectOutputTo(ih, 'rdoutReq')
            ih.connectOutputTo(eb, 'rdoutData')

        iit.connectOutputTo(gt, 'inIceTrigger')
        itt.connectOutputTo(gt, 'iceTopTrigger')

        gt.connectOutputTo(eb, 'glblTrigger')

        # build list of all nodes
        #
        allNodes = [gt, iit, itt, eb]
        for i in shList:
            allNodes.append(i)
        for i in ihList:
            allNodes.append(i)

        self.buildRunset(allNodes)

    def testComplex(self):
        # build nodes
        #
        a1 = Node('A', 1)
        a2 = Node('A', 2)
        b1 = Node('B', 1)
        b2 = Node('B', 2)
        c = Node('C')
        d = Node('D')
        e = Node('E')
        f = Node('F')
        g = Node('G')
        h = Node('H')
        i = Node('I')

        # connect nodes
        #
        a1.connectOutputTo(c, 'DataA')
        a2.connectOutputTo(c, 'DataA')
        b1.connectOutputTo(d, 'DataB')
        b2.connectOutputTo(d, 'DataB')

        c.connectOutputTo(e, 'DataC')
        d.connectOutputTo(f, 'DataD')
        e.connectOutputTo(f, 'DataE')
        f.connectOutputTo(g, 'DataF')
        g.connectOutputTo(h, 'DataG')
        h.connectOutputTo(e, 'BackH')
        h.connectOutputTo(i, 'DataH')

        # build list of all nodes
        #
        allNodes = [a1, a2, b1, b2, c, d, e, f, g, h, i]

        self.buildRunset(allNodes)

if __name__ == '__main__':
    unittest.main()
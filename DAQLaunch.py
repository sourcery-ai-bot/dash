#!/usr/bin/env python

#
# DAQ launch script - assumes deployment has occurred already.
# Run from an "experiment control" node - localhost/laptop or spXsX-expcont
#
# John Jacobsen, jacobsen@npxdesigns.com
# Started January, 2007

import sys
import optparse
from time import sleep
from os import environ, mkdir, system
from os.path import exists, isabs, join

from ClusterConfig \
    import ClusterConfig, ClusterConfigException, ConfigNotFoundException, \
           ConfigNotSpecifiedException
from DAQConfig import DAQConfig
from DAQConst import DAQPort
from DAQRPC import RPCClient
from GetIP import getIP
from Process import findProcess, processList

from ParallelShell import *

# the pDAQ release name
#
RELEASE = "1.0.0-SNAPSHOT"

# Find install location via $PDAQ_HOME, otherwise use locate_pdaq.py
if environ.has_key("PDAQ_HOME"):
    metaDir = environ["PDAQ_HOME"]
else:
    from locate_pdaq import find_pdaq_trunk
    metaDir = find_pdaq_trunk()

# add meta-project python dir to Python library search path
sys.path.append(join(metaDir, 'src', 'main', 'python'))
from SVNVersionInfo import get_version_info

SVN_ID = "$Id: DAQLaunch.py 4871 2010-01-26 14:56:01Z dglo $"

class HostNotFoundForComponent   (Exception): pass
class ComponentNotFoundInDatabase(Exception): pass

# Component Name -> JarParts mapping.  For constructing the name of
# the proper jar file used for running the component, based on the
# lower-case name of the component.
compNameJarPartsMap = {
    "eventbuilder"      : ("eventBuilder-prod", "comp"    ),
    "secondarybuilders" : ("secondaryBuilders", "comp"    ),
    "inicetrigger"      : ("trigger",           "iitrig"  ),
    "simpletrigger"     : ("trigger",           "simptrig"),
    "icetoptrigger"     : ("trigger",           "ittrig"  ),
    "globaltrigger"     : ("trigger",           "gtrig"   ),
    "amandatrigger"     : ("trigger",           "amtrig"  ),
    "stringhub"         : ("StringHub",         "comp"    ),
    "replayhub"         : ("StringHub",         "replay"  ),
    }

def getCompJar(compName):
    """ Return the name of the executable jar file for the named
    component.  """

    jarParts = compNameJarPartsMap.get(compName.lower(), None)
    if not jarParts:
        raise ComponentNotFoundInDatabase(compName)

    return "%s-%s-%s.jar" % (jarParts[0], RELEASE, jarParts[1])

def runCmd(cmd, parallel):
    if parallel is None:
        system(cmd)
    else:
        parallel.system(cmd)

def killJavaProcesses(dryRun, clusterConfig, verbose, killWith9, parallel=None):
    if parallel is None:
        parallel = ParallelShell(dryRun=dryRun, verbose=verbose, trace=verbose)
    for node in clusterConfig.nodes():
        for comp in node.components():
            jarName = getCompJar(comp.name())
            if killWith9: niner = "-9"
            else:         niner = ""
            if node.hostName() == "localhost": # Just kill it
                cmd = "pkill %s -fu %s %s" % (niner, environ["USER"], jarName)
                if verbose: print cmd
                parallel.add(cmd)
                if not killWith9:
                    cmd = "sleep 2; pkill -9 -fu %s %s" % \
                        (environ["USER"], jarName)
                    if verbose: print cmd
                    parallel.add(cmd)
            else:                            # Have to ssh to kill
                cmd = "ssh %s pkill %s -f %s" % \
                    (node.hostName(), niner, jarName)
                parallel.add(cmd)
                if not killWith9:
                    cmd = "sleep 2; ssh %s pkill -9 -f %s" % \
                        (node.hostName(), jarName)
                    parallel.add(cmd)

    if not dryRun:
        parallel.start()
        parallel.wait()

def startJavaProcesses(dryRun, clusterConfig, configDir, dashDir, logPort,
                       livePort, verbose, eventCheck, checkExists=True,
                       parallel=None):
    if parallel is None:
        parallel = ParallelShell(dryRun=dryRun, verbose=verbose, trace=verbose)

    # The dir where all the "executable" jar files are
    binDir = join(metaDir, 'target', 'pDAQ-%s-dist' % RELEASE, 'bin')
    if checkExists and not os.path.isdir(binDir):
        binDir = join(metaDir, 'target', 'pDAQ-%s-dist.dir' % RELEASE, 'bin')
        if not os.path.isdir(binDir):
            raise SystemExit("Cannot find jar file directory")

    # how are I/O streams handled?
    if not verbose:
        quietStr = " </dev/null >/dev/null 2>&1"
    else:
        quietStr = ""

    for node in clusterConfig.nodes():
        myIP = getIP(node.hostName())
        for comp in node.components():
            execJar = join(binDir, getCompJar(comp.name()))
            if checkExists and not exists(execJar):
                print "%s jar file does not exist: %s" % \
                    (comp.name(), execJar)
                continue

            javaCmd = comp.jvm()
            jvmArgs = comp.jvmArgs()

            switches = "-g %s" % configDir
            switches += " -c %s:%d" % (myIP, DAQPort.CNCSERVER)
            if logPort is not None:
                switches += " -l %s:%d,%s" % (myIP, logPort, comp.logLevel())
            if livePort is not None:
                switches += " -L %s:%d,%s" % (myIP, livePort, comp.logLevel())
            compIO = quietStr

            if comp.isHub():
                jvmArgs += " -Dicecube.daq.stringhub.componentId=%d" % \
                    comp.id()

            if eventCheck and comp.isBuilder():
                jvmArgs += " -Dicecube.daq.eventBuilder.validateEvents"

            if node.hostName() == "localhost": # Just run it
                cmd = "%s %s -jar %s %s %s &" % \
                    (javaCmd, jvmArgs, execJar, switches, compIO)
            else:                            # Have to ssh to run it
                cmd = \
                    """ssh -n %s \'sh -c \"%s %s -jar %s %s %s &\"%s &\'""" % \
                    (node.hostName(), javaCmd, jvmArgs, execJar, switches,
                     compIO, quietStr)

            if verbose: print cmd
            parallel.add(cmd)

    if verbose and not dryRun: parallel.showAll()
    if not dryRun:
        parallel.start()
        parallel.wait()

def reportAction(action, actionList, ignored):
    "Report which Python daemons were launched/killed and which were ignored"

    if len(actionList) > 0:
        if len(ignored) > 0:
            print "%s %s, ignored %s" % (action, ", ".join(actionList),
                                         ", ".join(ignored))
        else:
            print "%s %s" % (action, ", ".join(actionList))
    elif len(ignored) > 0:
        print "Ignored %s" % ", ".join(ignored)

def doKill(doLive, doDAQRun, doCnC, dryRun, dashDir, verbose, quiet,
           clusterConfig, killWith9, parallel=None):
    "Kill pDAQ python and java components in clusterConfig"
    if verbose: print "COMMANDS:"

    killed = []
    ignored = []

    batch = ((doLive, "DAQLive"),
             (doDAQRun, "DAQRun"),
             (doCnC, "CnCServer"))

    for b in batch:
        if b[0]:
            # Kill this program
            prog = join(dashDir, b[1] + '.py')
            cmd = prog + ' -k'
            if verbose: print cmd
            if not dryRun:
                runCmd(cmd, parallel)
                if not quiet: killed.append(b[1])
        elif not dryRun and not quiet:
            ignored.append(b[1])

    killJavaProcesses(dryRun, clusterConfig, verbose, killWith9, parallel)
    if verbose and not dryRun: print "DONE with killing Java Processes."
    if not quiet:
        reportAction("Killed", killed, ignored)

    # clear the active configuration
    clusterConfig.clearActiveConfig()

def isRunning(procName, procList):
    "Is this process running?"
    pids = list(findProcess(procName, procList))
    return len(pids) > 0

def doLaunch(doLive, doDAQRun, doCnC, dryRun, verbose, quiet, clusterConfig,
             dashDir, configDir, logDir, spadeDir, copyDir, logPort, livePort,
             eventCheck=False, checkExists=True, startMissing=True,
             parallel=None):
    "Launch components"

    # get a list of the running processes
    if not startMissing:
        procList = []
    else:
        procList = processList()

    launched = []
    ignored = []

    batch = ((doLive, "DAQLive"),
             (doDAQRun, "DAQRun"),
             (doCnC, "CnCServer"))

    for b in batch:
        doProg = b[0]
        progBase = b[1]
        progName = progBase + ".py"

        if startMissing and not doProg:
            doProg |= not isRunning(progName, procList)

        if doProg:
            path  = join(dashDir, progName)
            if progBase == "DAQLive":
                options = ""
                if verbose:
                    options += " -v"
                options += " &"
            elif progBase == "DAQRun":
                options = " -r -f -c %s -l %s -s %s -u %s" % \
                    (configDir, logDir, spadeDir, clusterConfig.configName)
                if livePort is not None:
                    if logPort is not None:
                        options += " -B"
                    else:
                        options += " -L"
                if copyDir: options += " -a %s" % copyDir
                if verbose: options += " -n &"
            elif progBase == "CnCServer":
                options = ""
                if logPort is not None:
                    options += ' -l localhost:%d' % logPort
                if livePort is not None:
                    options += ' -L localhost:%d' % livePort
                if verbose: options += ' &'
                else: options += ' -d'
            else:
                raise SystemExit("Cannot launch program \"%s\"" % progBase)

            cmd = "%s%s" % (path, options)
            if verbose: print cmd
            if not dryRun:
                runCmd(cmd, parallel)
                if not quiet: launched.append(progBase)

            if verbose and progBase == "DAQRun":
                sleep(5) # Fixme - this is a little kludgy, but CnCServer
                         # won't log correctly if DAQRun isn't launched.

        elif not dryRun and not quiet:
            ignored.append(progBase)

    startJavaProcesses(dryRun, clusterConfig, configDir, dashDir, logPort,
                       livePort, verbose, eventCheck, checkExists=checkExists,
                       parallel=parallel)
    if verbose and not dryRun: print "DONE with starting Java Processes."
    if not quiet:
        reportAction("Launched", launched, ignored)

    # remember the active configuration
    clusterConfig.writeCacheFile(True)

def cyclePDAQ(dashDir, clusterConfig, configDir, logDir, spadeDir, copyDir,
              logPort, livePort, eventCheck=False, checkExists=True,
              startMissing=True, parallel=None):
    """
    Stop and restart pDAQ programs - can be used by DAQRun when cycling
    pDAQ in an attempt to wipe the slate clean after a failure
    """
    doCnC = True
    doDAQRun = False
    doLive = False
    dryRun = False
    verbose = False
    quiet = True
    killWith9 = False

    doKill(doLive, doDAQRun, doCnC, dryRun, dashDir, verbose, quiet,
           clusterConfig, killWith9, parallel)
    doLaunch(doLive, doDAQRun, doCnC, dryRun, verbose, quiet, clusterConfig,
             dashDir, configDir, logDir, spadeDir, copyDir, logPort, livePort,
             eventCheck=eventCheck, checkExists=checkExists,
             startMissing=startMissing, parallel=parallel)

if __name__ == "__main__":
    LOGMODE_OLD = 1
    LOGMODE_LIVE = 2
    LOGMODE_BOTH = LOGMODE_OLD | LOGMODE_LIVE

    ver_info = "%(filename)s %(revision)s %(date)s %(time)s %(author)s %(release)s %(repo_rev)s" % get_version_info(SVN_ID)
    usage = "%prog [options]\nversion: " + ver_info
    p = optparse.OptionParser(usage=usage, version=ver_info)

    p.add_option("-B", "--log-to-files-and-i3live", action="store_const",
                  const=LOGMODE_BOTH, dest="logMode",
                 help="Send log messages to both I3Live and to local files")
    p.add_option("-C", "--cluster-desc",  action="store", type="string",
                 dest="clusterDesc",
                 help="Cluster description name.")
    p.add_option("-c", "--config-name",  action="store", type="string",
                 dest="clusterConfigName",
                 help="Cluster configuration name, subset of deployed configuration.")
    p.add_option("-e", "--event-check",  action="store_true", dest="eventCheck",
                 help="Event builder will validate events")
    p.add_option("-f", "--force",        action="store_true", dest="force",
                 help="kill components even if there is an active run")
    p.add_option("-k", "--kill-only",    action="store_true", dest="killOnly",
                 help="Kill pDAQ components, don't restart")
    p.add_option("-l", "--list-configs", action="store_true", dest="doList",
                 help="List available configs")
    p.add_option("-L", "--log-to-i3live", action="store_const",
                 const=LOGMODE_LIVE, dest="logMode",
                 help="Send all log messages to I3Live")
    p.add_option("-n", "--dry-run",      action="store_true", dest="dryRun",
                 help="\"Dry run\" only, don't actually do anything")
    p.add_option("-O", "--log-to-files", action="store_const",
                 const=LOGMODE_OLD, dest="logMode",
                 help="Send log messages to local files")

    p.add_option("-q", "--quiet",        action="store_true", dest="quiet",
                 help="Don't print actions")
    p.add_option("-s", "--skip-kill",    action="store_true", dest="skipKill",
                 help="Don't kill anything, just launch")
    p.add_option("-v", "--verbose",      action="store_true", dest="verbose",
                 help="Log output for all components to terminal")
    p.add_option("-9", "--kill-kill",    action="store_true", dest="killWith9",
                 help="just kill everything with extreme (-9) prejudice")
    p.set_defaults(clusterConfigName = None,
                   dryRun            = False,
                   verbose           = False,
                   doList            = False,
                   skipKill          = False,
                   killWith9         = False,
                   killOnly          = False,
                   eventCheck        = False,
                   force             = False,
                   quiet             = False,
                   logMode           = LOGMODE_BOTH)
    opt, args = p.parse_args()

    if opt.quiet and opt.verbose:
        print >>sys.stderr, "Cannot specify both -q(uiet) and -v(erbose)"
        raise SystemExit

    if (opt.logMode & LOGMODE_OLD) == LOGMODE_OLD:
        logPort = DAQPort.CATCHALL
    else:
        logPort = None

    if (opt.logMode & LOGMODE_LIVE) == LOGMODE_LIVE:
        livePort = DAQPort.I3LIVE
    else:
        livePort = None

    configDir = join(metaDir, 'config')
    logDir    = join(' ', 'mnt', 'data', 'pdaq', 'log').strip()
    logDirFallBack = join(metaDir, 'log')
    dashDir   = join(metaDir, 'dash')

    if not opt.force:
        # connect to CnCServer
        cncrpc = RPCClient('localhost', DAQPort.CNCSERVER)

        # Get the number of active runsets from CnCServer
        try:
            numSets = int(cncrpc.rpc_num_sets())
        except:
            numSets = None

        if numSets is not None and numSets > 0:
            daqrpc = RPCClient("localhost", DAQPort.DAQRUN)
            try:
                state  = daqrpc.rpc_run_state()
            except:
                state = "DEAD"

            deadStates = ("DEAD", "ERROR", "STOPPED")
            if not state in deadStates:
                if numSets == 1:
                    plural = ''
                else:
                    plural = 's'
                print >>sys.stderr, 'Found %d %s runset%s' % \
                    (numSets, state.lower(), plural)
                print >>sys.stderr, \
                    'To force a restart, rerun with the --force option'
                raise SystemExit

    if opt.doList:
        DAQConfig.showList(None, None)
        raise SystemExit

    if not opt.skipKill:
        doLive = opt.killOnly
        doRun = True
        doCnC = True

        try:
            activeConfig = DAQConfig.getClusterConfiguration(None, False, True)
            doKill(doLive, doRun, doCnC, opt.dryRun, dashDir, opt.verbose,
                   opt.quiet, activeConfig, opt.killWith9)
        except ClusterConfigException:
            if opt.killOnly: print >>sys.stderr, 'DAQ is not currently active'

    if not opt.killOnly:
        clusterConfig = DAQConfig.getClusterConfiguration(opt.clusterConfigName,
                                                          opt.doList, False,
                                                          opt.clusterDesc)
        if opt.doList: raise SystemExit

        if opt.verbose:
            print "Version: %(filename)s %(revision)s %(date)s %(time)s " \
                "%(author)s %(release)s %(repo_rev)s" % get_version_info(SVN_ID)
            print "CONFIG: %s" % clusterConfig.configName

            nodeList = clusterConfig.nodes()
            nodeList.sort()

            print "NODES:"
            for node in nodeList:
                print "  %s(%s)" % (node.hostName(), node.locName()),

                compList = node.components()
                compList.sort()

                for comp in compList:
                    print "%s#%d " % (comp.name(), comp.id()),
                print

        spadeDir  = clusterConfig.logDirForSpade()
        # Assume non-fully-qualified paths are relative to metaproject top dir:
        if not isabs(spadeDir):
            spadeDir = join(metaDir, spadeDir)

        if not exists(spadeDir) and not opt.dryRun: mkdir(spadeDir)

        copyDir   = clusterConfig.logDirCopies()
        # Assume non-fully-qualified paths are relative to metaproject top dir:
        if copyDir:
            if not isabs(copyDir):
                copyDir = join(metaDir, copyDir)
            if not exists(copyDir) and not opt.dryRun: mkdir(copyDir)

        # Set up logDir
        if not exists(logDir):
            if not opt.dryRun:
                try:
                    mkdir(logDir)
                except OSError, (errno, strerror):
                    if opt.verbose:
                        print "Problem making log dir: '%s' (%s)" % \
                            (logDir, strerror)
                        print "Using fallback for logDir: %s" % (logDirFallBack)
                    logDir = logDirFallBack
                    if not exists(logDir): mkdir(logDir)
        else:
            system('rm -f %s' % join(logDir, 'catchall.log'))

        doLive = False
        doRun = True
        doCnC = True

        doLaunch(doLive, doRun, doCnC, opt.dryRun, opt.verbose, opt.quiet,
                 clusterConfig, dashDir, configDir, logDir, spadeDir, copyDir,
                 logPort, livePort, eventCheck=opt.eventCheck, checkExists=True,
                 startMissing=True)

#!/usr/bin/env python

# GPSCheck.py
# John Jacobsen, NPX Designs, Inc., jacobsen\@npxdesigns.com
# Started: Fri Jun  1 15:57:10 2007

import sys, optparse
from os import environ
from os.path import join

from DAQConfig import DAQConfigParser
from ParallelShell import ParallelShell

def main():
    "Main program"
    usage = "%prog [options]"
    p = optparse.OptionParser(usage=usage)
    p.add_option("-c", "--config-name", type="string", dest="clusterConfigName",
                 action="store", default=None,
                 help="REQUIRED: Configuration name")
    p.add_option("-n", "--dry-run", dest="dryRun",
                 action="store_true", default=False,
                 help="Don't actually run DAQGPS - just print what" +
                 " would be done")
    p.add_option("-l", "--list-configs", dest="doList",
                 action="store_true", default=False,
                 help="List available configs")
    
    opt, args = p.parse_args()

    config = DAQConfigParser.getClusterConfiguration(opt.clusterConfigName,
                                                     opt.doList)
    if opt.doList: raise SystemExit

    hublist = config.getHubNodes()

    cmds = ParallelShell(dryRun = opt.dryRun, timeout = 20)
    ids = {}
    for hub in hublist:
        # FIXME - actually implement the right thing here
        cmd = "ssh %s readgps -h" % hub
        ids[hub] = (cmds.add(cmd))

    cmds.start()
    cmds.wait()

    for hub in hublist:
        print "Hub %s: " % hub,
        result = cmds.getResult(ids[hub])
        result = result.rstrip()
        print result

        # Parse template:
        # 2 pairs plugged, 2 powered; 4 DOMs communicating, 4 in iceboot
        #match = re.search(r'(\d+) pairs plugged, (\d+) powered; (\d+) DOMs communicating, (\d+) in iceboot',
        #                  result)

            
if __name__ == "__main__": main()


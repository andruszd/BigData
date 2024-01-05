#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = ""
__copyright__ = ""
__credits__ = [""]
__license__ = "GPL3"
__version__ = "0.0.1"
__maintainer__ = ""
__email__ = ""
__status__ = "Prototype"

# Imports
# Default Imports
import os
import sys
import argparse
import gc
import traceback
# Optional Imports
# Uncomment as needed.
#
# from pathlib import Path
# import configparser
# import psutil
import requests
# import json
# import re
# import time
# from pywebhdfs.webhdfs import PyWebHdfsClient
from icecream import ic
# from typing import List, Union
# from prettytable import PrettyTable
from datetime import datetime

# from deepdiff import DeepDiff

# Variables
error_flag = 0
# if you add a new check to the list below, you need to add it to the check_all function.
checks = ["check_all", "check_status", "check_space", "check_blocks", "check_replication", "check_namenodes",
          "check_datanodes", "check_journalnodes", "check_logs"]


# Set the time format for icecream
def time_format():
    return f'{datetime.now()}|> '


# Set the icecream debug on or off
# Hash out where appropriate to turn off
# Default is on
ic.enable()
# ic.disable()
ic.configureOutput(includeContext=True)
ic.configureOutput(prefix=time_format)


def calculate_percentage(num1, num2):
    try:
        return (num1 / num2) * 100
    except ZeroDivisionError:
        return 0


def check_all(server_name, ef):
    ic("check_all function on server:", server_name)
    # Check if server is up, if not exit with error(all I want is 200).
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    ic(response)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
        for check in checks:
            ic(check)
            if check == "check_all":
                ic("Skipping check_all")
            else:
                ic(check)
                match check:
                    case "check_status":
                        check_status(server_name, ef)
                    case "check_space":
                        check_space(server_name, ef)
                    case "check_blocks":
                        check_blocks(server_name, ef)
                    case "check_replication":
                        check_replication(server_name, ef)
                    case "check_namenodes":
                        check_namenodes(server_name, ef)
                    case "check_datanodes":
                        check_datanodes(server_name, ef)
                    case "check_journalnodes":
                        check_journalnodes(server_name, ef)
                    case "check_logs":
                        check_logs(server_name, ef)
                    # You should never get here.
                    case _:
                        ic("No Check Selected")
                        ic("Please select a check from the following list:")
                        ic(checks)
                        sys.exit(1)
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1  # Error
    return ef


def check_status(server_name, ef):
    ic("check_status function on server:", server_name)
    # Check if server is up, if not exit with error(all I want is 200).
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    ic(response)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        print("-------------------------------------")
        print("Status of ", {server_name}, ": UP")
        print("-------------------------------------")
        ef: int = 0  # No error
    else:
        ic(server_name, "Server is down")
        print("-------------------------------------")
        print("Status of ", {server_name}, ": DOWN")
        print("-------------------------------------")
        response.raise_for_status()
        ef: int = 1  # Error
    return ef


def check_space(server_name, ef):
    global error_flag
    ic("check_space function on server:", server_name)
    # Capacity variables are in GB size.
    capacitytotal: int = 0
    capacityused: int = 0
    capacityremaining: int = 0
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
        error_flag = ef
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1
        error_flag = ef
        return error_flag  # Error
    ic(response)
    response = response.json()
    p_used = round(
        calculate_percentage(response['beans'][0]['CapacityUsedGB'], response['beans'][0]['CapacityTotalGB']), 2)
    p_remaining = round(calculate_percentage(response['beans'][0]['CapacityRemainingGB'],
                                             response['beans'][0]['CapacityTotalGB']), 2)
    ic("Capacity Total:", response['beans'][0]['CapacityTotalGB'], "GB")
    ic("Capacity Used:", response['beans'][0]['CapacityUsedGB'], "GB")
    ic("Capacity Remaining:", response['beans'][0]['CapacityRemainingGB'], "GB")
    print("-------------------------------------")
    print("Available Disk Space Across Cluster:")
    print("-------------------------------------")
    print("Capacity Total:", response['beans'][0]['CapacityTotalGB'], "GB")
    if response['beans'][0]['CapacityUsedGB'] > 0:
        print("Capacity Used:", response['beans'][0]['CapacityUsedGB'], "GB" "or", p_used, "%")
        print("-------------------------------------")
    else:
        print("Capacity Used:", response['beans'][0]['CapacityUsed'], "Bytes", "or", p_used, "%")
    print("Capacity Remaining:", response['beans'][0]['CapacityRemainingGB'], "GB", "or", p_remaining, "%")
    print("-------------------------------------")
    error_flag = ef
    return ef


def check_blocks(server_name, ef):
    ic("check_blocks function on server:", server_name)
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1  # Error
        return ef
    ic(response)
    response = response.json()
    print("-------------------------------------")
    print("Status of Blocks Across Cluster:")
    print("-------------------------------------")
    print("Block Information:->")
    print("Block Capacity:", response['beans'][0]['BlockCapacity'])
    print("Excess Blocks:", response['beans'][0]['ExcessBlocks'])
    print("Total Blocks:", response['beans'][0]['BlocksTotal'])
    print("")
    print("Corruption Information:->")
    print("Corrupted Blocks:", response['beans'][0]['CorruptBlocks'])
    print("Corrupted Replicated Blocks:", response['beans'][0]['CorruptReplicatedBlocks'])
    print("")
    print("Missing Block Information:->")
    print("Missing Blocks:", response['beans'][0]['MissingBlocks'])
    print("Missing ReplOne Blocks:", response['beans'][0]['MissingReplOneBlocks'])
    print("Missing Replicated Blocks:", response['beans'][0]['MissingReplicatedBlocks'])
    print("Missing ReplicationOne Blocks:", response['beans'][0]['MissingReplicationOneBlocks'])
    print("-------------------------------------")
    return ef


def check_replication(server_name, ef):
    ic("check_replication function on server:", server_name)
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1  # Error
        return ef
    ic(response)
    response = response.json()
    print("-------------------------------------")
    print("Status of Replication Across Cluster:")
    print("-------------------------------------")
    print("Block Information:->")
    print("Total Blocks Replicated :", response['beans'][0]['TotalReplicatedBlocks'])
    print("Blocks Under Replicated:", response['beans'][0]['UnderReplicatedBlocks'])
    print("Total ECBlock Groups:", response['beans'][0]['TotalECBlockGroups'])
    print("Bytes In Future Replicated Blocks:", response['beans'][0]['BytesInFutureReplicatedBlocks'])
    print("Bytes In Future ECBlock Groups:", response['beans'][0]['BytesInFutureECBlockGroups'])
    print("")
    print("Corruption Information:->")
    print("Corrupt Replicated Blocks:", response['beans'][0]['CorruptReplicatedBlocks'])
    print("Corrupt ECBlock Groups:", response['beans'][0]['CorruptECBlockGroups'])
    print("Missing Replicated Blocks:", response['beans'][0]['MissingReplicatedBlocks'])
    #    print("Missing Replication One Blocks:", response['beans'][0][' MissingReplicationOneBlocks'])
    print("Missing ECBlock Groups", response['beans'][0]['MissingECBlockGroups'])
    print("Blocks Postponed and/or Misreplicated :", response['beans'][0]['PostponedMisreplicatedBlocks'])
    print("")
    print("Pending Information:->")
    print("Blocks Scheduled for Replication :", response['beans'][0]['ScheduledReplicationBlocks'])
    print("Blocks Pending Replication :", response['beans'][0]['PendingReplicationBlocks'])
    print("Blocks Pending Reconstruction :", response['beans'][0]['PendingReconstructionBlocks'])
    print("Blocks Pending Deletion:", response['beans'][0]['PendingDeletionBlocks'])
    print("Blocks Pending Deletion Replicated:", response['beans'][0]['PendingDeletionReplicatedBlocks'])
    print("Blocks Pending Deletion ECBlocks:", response['beans'][0]['PendingDeletionECBlocks'])
    #    print("Blocks Pending Replication In Open Files:", response['beans'][0]['PendingReplicationBlocksInOpenFiles'])
    print("")
    print("Redundancy Information:->")
    print("Low Redundancy Blocks:", response['beans'][0]['LowRedundancyBlocks'])
    print("Low Redundancy Replicated Blocks:", response['beans'][0]['LowRedundancyReplicatedBlocks'])
    print("Low Redundancy ECBlock Groups:", response['beans'][0]['LowRedundancyECBlockGroups'])
    print("Highest Priority Low Redundancy Replicated Blocks:",
          response['beans'][0]['HighestPriorityLowRedundancyReplicatedBlocks'])
    print("Highest Priority LowRedundancy ECBlocks:", response['beans'][0]['HighestPriorityLowRedundancyECBlocks'])
    print("")
    print("-------------------------------------")
    return ef


def check_namenodes(server_name, ef):
    ic("check_namenodes function on server:", server_name)
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1  # Error
        return ef
    ic(response)
    response = response.json()
    print("-------------------------------------")
    print("Status of NameNodes Across Cluster:")
    print("-------------------------------------")
    print("NameNode Information:->")
    print("State:", response['beans'][0]['State'])
    print("NNRole:", response['beans'][0]['NNRole'])
    print("Host And Port:", response['beans'][0]['HostAndPort'])
    #    print("Security Enable:", response['beans'][0]['SecurityEnable'])
    print("Last HA Transition Time:", response['beans'][0]['LastHATransitionTime'])
    #    print("Expired Heartbeats:", response['beans'][0]['ExpiredHeartbeats'])
    #    print("Number of DataNodes In Maintenance which are Live :", response['beans'][0]['NumInMaintenanceLiveDataNodes'])
    #    print("Number of DataNodes In Maintenance which are Dead :", response['beans'][0]['NumInMaintenanceDeadDataNodes'])
    #    print("Number of DataNodes Entering Maintenance :", response['beans'][0]['NumEnteringMaintenanceDataNodes'])
    print("Bytes With Future Generation Stamps:", response['beans'][0]['BytesWithFutureGenerationStamps'])
    print("Slow Peers:", response['beans'][0]['SlowPeersReport'])
    print("Slow Disks:", response['beans'][0]['SlowDisksReport'])
    print("")
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1  # Error
        return ef
    ic(response)
    response = response.json()
    print("")
    print("Node Status Information:->")
    print("Number of Live Data Nodes:", response['beans'][0]['NumLiveDataNodes'])
    print("Number of Stale DataNodes:", response['beans'][0]['StaleDataNodes'])
    print("Number of DataNodes In Service which are Live:", response['beans'][0]['NumInServiceLiveDataNodes'])
    print("Number of Dead DataNodes:", response['beans'][0]['NumDeadDataNodes'])
    print("Number of DataNodes Decommissioning:", response['beans'][0]['NumDecommissioningDataNodes'])
    print("Number of Decommissioned Live DataNodes:", response['beans'][0]['NumDecomLiveDataNodes'])
    print("Number of Decommissioned Dead DataNodes:", response['beans'][0]['NumDecomDeadDataNodes'])
    print("")
    print("Storage Information:->")
    print("Volume Failures Total:", response['beans'][0]['VolumeFailuresTotal'])
    print("Estimated Capacity Lost Total:", response['beans'][0]['EstimatedCapacityLostTotal'])
    #    print("Num Stale Storages:", response['beans'][0][' NumStaleStorages'])
    print("-------------------------------------")
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1  # Error
        return ef
    ic(response)
    response = response.json()
    print("")
    print("-------------------------------------")
    print("Advanced NameNode Information")
    print("-------------------------------------")
    print("Verify EC With Topology Result:", response['beans'][0]['VerifyECWithTopologyResult'])
    print("Live Nodes:", response['beans'][0]['LiveNodes'])
    print("Dead Nodes:", response['beans'][0]['DeadNodes'])
    print("Decommissioned Nodes:", response['beans'][0]['DecomNodes'])
    print("Entering  Maintenance Nodes:", response['beans'][0]['EnteringMaintenanceNodes'])
    print("Journal Transaction Info:", response['beans'][0]['JournalTransactionInfo'])
    print("Node Usage:", response['beans'][0]['NodeUsage'])
    print("Safemode:", response['beans'][0]['Safemode'])
    print("-------------------------------------")
    return ef


def check_datanodes(server_name, ef):
    ic("check_datanodes function on server:", server_name)
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1  # Error
        return ef
    ic(response)
    response = response.json()
    print("-------------------------------------")
    print("Info For ", {server_name})
    print("-------------------------------------")
    print("DataNode Information:->")
    print("BP Service Actor Info", response['beans'][0]['BPServiceActorInfo'])
    print("Volume Info", response['beans'][0]['VolumeInfo'])
    print("Disk Balancer Status", response['beans'][0]['DiskBalancerStatus'])
    print("Security Enabled", response['beans'][0]['SecurityEnabled'])
    print("Send Packet Downstream Avg Info", response['beans'][0]['SendPacketDownstreamAvgInfo'])
    print("Slow Disks", response['beans'][0]['SlowDisks'])
    print("-------------------------------------")

    ic("check_datanodes function on server:", server_name)
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=DataNode,name=FSDatasetState"
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1  # Error
        return ef
    ic(response)
    response = response.json()
    print("-------------------------------------")
    print("Status For ", {server_name})
    print("-------------------------------------")
    print("Capacity", response['beans'][0]['Capacity'])
    print("Dfs Used", response['beans'][0]['DfsUsed'])
    print("Remaining", response['beans'][0]['Remaining'])
    print("Num Failed Volumes", response['beans'][0]['NumFailedVolumes'])
    print("Last Volume Failure Date", response['beans'][0]['LastVolumeFailureDate'])
    print("Estimated Capacity Lost Total", response['beans'][0]['EstimatedCapacityLostTotal'])
    print("Cache Used", response['beans'][0]['CacheUsed'])
    print("Cache Capacity", response['beans'][0]['CacheCapacity'])
    print("Num Blocks Cached", response['beans'][0]['NumBlocksCached'])
    print("Num Blocks Failed To Cache", response['beans'][0]['NumBlocksFailedToCache'])
    print("Num Blocks Failed To UnCache", response['beans'][0]['NumBlocksFailedToUnCache'])
    print("-------------------------------------")
    return ef


def check_journalnodes(server_name, ef):
    ic("check_journalnodes function on server:", server_name)
    url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=DataNode,name="
    ic(url)
    response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
    if response.status_code == requests.codes.ok:
        ic(server_name, "Server is up")
        ef: int = 0  # No error
    else:
        ic(server_name, "Server is down")
        response.raise_for_status()
        ef: int = 1  # Error
        return ef
    ic(response)
    response = response.json()
    print("-------------------------------------")
    print("Status of JournalNodes Across Cluster:")
    print("-------------------------------------")
    print("JournalNode Information:->")
    print("-------------------------------------")
    print("No Current Checks for JournalNodes")
    print("-------------------------------------")
    return ef


def check_logs(server_name, node, ef):
    match node:
        case "nn":
            ic("check_logs function on server:", server_name)
            url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=NameNode,name=JvmMetrics"
            ic(url)
            response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
            if response.status_code == requests.codes.ok:
                ic(server_name, "Server is up")
                ef: int = 0  # No error
            else:
                ic(server_name, "Server is down")
                response.raise_for_status()
                ef: int = 1  # Error
                return ef
            ic(response)
            response = response.json()
            print("-------------------------------------")
            print("Status of Logs for:", {server_name})
            print("-------------------------------------")
            print("Log Information:->")
            print("Number of Fatal Messages:", response['beans'][0]['LogFatal'])
            print("Number of Error Messages:", response['beans'][0]['LogError'])
            print("Number of Warn  Messages:", response['beans'][0]['LogWarn'])
            print("Number of Info  Messages:", response['beans'][0]['LogInfo'])
            #    print("Number of Debug Messages:", response['beans'][0]['LogDebug'])
            #    print("Number of Trace Messages:", response['beans'][0]['LogTrace'])
            #    print("Number of Total Messages:", response['beans'][0]['LogTotal'])
            print("-------------------------------------")
            return ef
        case "dn":
            url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=DataNode,name=JvmMetrics"
            ic(url)
            response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
            if response.status_code == requests.codes.ok:
                ic(server_name, "Server is up")
                ef: int = 0  # No error
            else:
                ic(server_name, "Server is down")
                response.raise_for_status()
                ef: int = 1  # Error
                return ef
            ic(response)
            response = response.json()
            print("-------------------------------------")
            print("Status of Logs for:", {server_name})
            print("-------------------------------------")
            print("Log Information:->")
            print("Number of Fatal Messages:", response['beans'][0]['LogFatal'])
            print("Number of Error Messages:", response['beans'][0]['LogError'])
            print("Number of Warn  Messages:", response['beans'][0]['LogWarn'])
            print("Number of Info  Messages:", response['beans'][0]['LogInfo'])
            #    print("Number of Debug Messages:", response['beans'][0]['LogDebug'])
            #    print("Number of Trace Messages:", response['beans'][0]['LogTrace'])
            #    print("Number of Total Messages:", response['beans'][0]['LogTotal'])
            print("-------------------------------------")
            return ef
        case "jn":
            url: str = f"http://{server_name}:9870/jmx?qry=Hadoop:service=JournalNode,name=JvmMetrics"
            ic(url)
            response = requests.get(url, timeout=5, verify=False, allow_redirects=False)
            if response.status_code == requests.codes.ok:
                ic(server_name, "Server is up")
                ef: int = 0
            else:
                ic(server_name, "Server is down")
                response.raise_for_status()
                ef: int = 1
                return ef
            ic(response)
            response = response.json()
            print("-------------------------------------")
            print("Status of Logs for:", {server_name})
            print("-------------------------------------")
            print("Log Information:->")
            print("Number of Fatal Messages:", response['beans'][0]['LogFatal'])
            print("Number of Error Messages:", response['beans'][0]['LogError'])
            print("Number of Warn  Messages:", response['beans'][0]['LogWarn'])
            print("Number of Info  Messages:", response['beans'][0]['LogInfo'])
            print("-------------------------------------")
            return ef


def main(warning=None, critical=None):
    global error_flag

    # Set defaults
    which_check = "status"
    server = "tardis"
    port = 666
    user = "dr.who"
    warning = 100
    critical = 100
    node = "nn"
    # Setup traceback
    type, val, tb = None, None, None
    try:
        # Validate input
        parser = argparse.ArgumentParser(prog='Hadoop Health Check',
                                         description='Provides a number of health checks on a Hadoop Cluster',
                                         epilog='Example: check_hadoop.py -v -s server_name -p port -u user_name '
                                                '-ch check_name -w warning -c critical ')
        parser.add_argument("-s", "--server", help="You need to provide the server name or ip address",
                            type=str, required=True)
        parser.add_argument("-p", "--port", help="You need to provide the port number", type=int, required=True)
        parser.add_argument("-u", "--user", help="You need to provide the user name", type=str, required=True)
        parser.add_argument("-n", "--node", help="You need to provide the node name", type=str,
                            choices=["an", "nn", "dn", "jn"], required=True)
        parser.add_argument("-ch", "--check", help="You need to provide the name of the Check you want to Run",
                            type=str, choices=["all", "status", "space", "blocks", "replication", "namenodes",
                                               "datanodes", "journalnodes", "logs"], required=True)
        parser.add_argument("-w", "--waring", help="You need to provide the Warning %", type=int,
                            default=80, choices=range(40, 95), required=False)
        parser.add_argument("-c", "--critical", help="You need to provide the Critical %", type=int,
                            default=90, choices=range(60, 99), required=False)
        parser.add_argument("-v", "--verbose", help="increase output verbosity", action='store_true',
                            required=False)
        parser.add_argument('--version', action='version', version='%(prog)s Version:1')
        arg = parser.parse_args()
        server = arg.server
        port = arg.port
        user = arg.user
        node = arg.node
        which_check = arg.check
        warning = arg.waring
        critical = arg.critical
        action = arg.verbose
        # Check if we want to run in verbose mode.
        if action:
            ic.enable()
            ic("Verbose output. Debugging mode")
        else:
            ic.disable()
        ic("Starting Main")
        ic("Checking :", server, port, user, node, which_check, warning, critical)

        match node:
            case "an":
                match which_check:
                    case "all":
                        check_all(server, error_flag)
                    case "status":
                        check_status(server, error_flag)
                    case "logs":
                        check_logs(server, node, error_flag)
                    # You should never get here.
                    case _:
                        ic("No Check Selected")
                        ic("Please select a check from the following list:")
                        ic(checks)
                        sys.exit(1)
            case "nn":
                match which_check:
                    case "space":
                        check_space(server, error_flag)
                    case "blocks":
                        check_blocks(server, error_flag)
                    case "replication":
                        check_replication(server, error_flag)
                    case "namenodes":
                        check_namenodes(server, error_flag)
                    # You should never get here.
                    case _:
                        ic("No Check Selected")
                        ic("Please select a check from the following list:")
                        ic(checks)
                        sys.exit(1)
            case "dn":
                match which_check:
                    case "datanodes":
                        check_datanodes(server, error_flag)
                    # You should never get here.
                    case _:
                        ic("No Check Selected")
                        ic("Please select a check from the following list:")
                        ic(checks)
                        sys.exit(1)
            case "jn":
                match which_check:
                    case "journalnodes":
                        check_journalnodes(server, error_flag)
                    # You should never get here.
                    case _:
                        ic("No Check Selected")
                        ic("Please select a check from the following list:")
                        ic(checks)
                        sys.exit(1)
            # You should never get here.
            case _:
                ic("No Check Selected")
                ic("Please select a check from the following list:")
                ic(checks)
                sys.exit(1)

    except Exception as e:
        type, val, tb = sys.exc_info()
        # Clear all trace frames.
        traceback.clear_frames(tb)
        # some cleanup code
    gc.collect()
    # and then use the tb:
    if tb:
        ic("Exception Error ->")
        error_flag = 1  # Error
        raise type(val).with_traceback(tb)


if __name__ == '__main__':
    main()
    if error_flag == 1:
        ic("## OPPS! ##")
        ic("Exception Error ->")
        sys.exit(1)
    else:
        ic("* Complete! *")
        ic("All OK")

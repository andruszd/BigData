Hadoop Health Check

This script will check the health of the cluster.
Hadoop provides a Java JMX interface to monitor the health of the cluster and can be accessed via JConsole or url.

The JMX interface is available at http://<namenode>:9870/jmx and http://<datanode>:10064/jmx 
for namenode and datanode respectively.

The script will check the following parameters:
1. Hadoop Cluster Status
2. Hadoop Cluster Used Space
3. Hadoop Cluster Total Space
4. Hadoop Cluster Free Space
5. Hadoop Cluster Live Nodes
6. Hadoop Cluster Dead Nodes
7. Hadoop Cluster Number of Rebooted Nodes
8. Hadoop Cluster Number of Lost Nodes
9. Hadoop Cluster Number of Replaced Nodes
10. Hadoop Cluster Number of Stale Nodes
11. Hadoop Cluster Number of Shutdown Nodes
12. Hadoop Cluster Number of Decommissioning Nodes
13. Hadoop Cluster Number of Decommissioned Nodes
14. Hadoop Cluster Number of Entering Maintenance Nodes
15. Hadoop Cluster Number of In Maintenance Nodes

To access the JMX interface, the script will use http requests to access the url and parse the json output.
To access the JMX information you need to know the name of the bean and the attribute you want to access.
The bean name is the name of the service and the attribute is the name of the metric you want to access.
To see the list of beans and attributes, you can access the url http://<namenode>:9870/jmx and http://<datanode>:10064/jmx
This will give you a list of all the beans and attributes available.

So if you want to access the number of live nodes, you need to access the bean "Hadoop:service=NameNode,name=NameNodeInfo" and the attribute "NumLiveDataNodes"

Here is  an example of the output for all the parameters for JvmMetrics
To access the bean and attribute, you need to use the following url http://<namenode>:9870/jmx?qry=Hadoop:service=DataNode,name=JvmMetrics
The output will be in json format and will look like this:
 
 "beans" : [ {
    "name" : "Hadoop:service=DataNode,name=JvmMetrics",
    "modelerType" : "JvmMetrics",
    "tag.Context" : "jvm",
    "tag.ProcessName" : "DataNode",
    "tag.SessionId" : null,
    "tag.Hostname" : "hdfsdn01",
    "MemNonHeapUsedM" : 53.67965,
    "MemNonHeapCommittedM" : 56.6875,
    "MemNonHeapMaxM" : -1.0,
    "MemHeapUsedM" : 25.831047,
    "MemHeapCommittedM" : 50.765625,
    "MemHeapMaxM" : 407.9375,
    "MemMaxM" : 407.9375,
    "GcCountCopy" : 134,
    "GcTimeMillisCopy" : 269,
    "GcCountMarkSweepCompact" : 2,
    "GcTimeMillisMarkSweepCompact" : 84,
    "GcCount" : 136,
    "GcTimeMillis" : 353,
    "GcNumWarnThresholdExceeded" : 0,
    "GcNumInfoThresholdExceeded" : 0,
    "GcTotalExtraSleepTime" : 4225,
    "ThreadsNew" : 0,
    "ThreadsRunnable" : 11,
    "ThreadsBlocked" : 0,
    "ThreadsWaiting" : 6,
    "ThreadsTimedWaiting" : 27,
    "ThreadsTerminated" : 0,
    "LogFatal" : 0,
    "LogError" : 6,
    "LogWarn" : 4,
    "LogInfo" : 80
    } ]




To access the JMX interface, you need to enable it in the hadoop configuration files.
For the namenode, you need to enable the following properties in the hdfs-site.xml file:
```
<property>
  <name>dfs.namenode.http-address</name>
  <value>
    <namenode>:9870
    </value>
</property>
<property>
  <name>dfs.namenode.secondary.http-address</name>
  <value>
    <namenode>:9868
    </value>
</property>
<property>
  <name>dfs.namenode.jmxremote.port</name>
  <value>8004</value>
</property>
<property>
  <name>dfs.namenode.jmxremote.authenticate</name>
  <value>false</value>
</property>
<property>
  <name>dfs.namenode.jmxremote.ssl</name>
  <value>false</value>
</property>
```

For more information on the JMX interface,

see https://hadoop.apache.org/docs/r2.7.3/hadoop-project-dist/hadoop-common/Metrics.html for more information.

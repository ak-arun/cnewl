nohup java -cp /home/hdfs/otg/lib/onetimegrabber-0.0.1.jar::/usr/hdp/current/hive-client/lib/*:/usr/hdp/current/hadoop-client/lib/*:/usr/hdp/current/hadoop-client/* com.ak.hive.ddlgrabber.onetimegrabber.HiveDDLOnetimeGrabber /home/hdfs/otg/conf/grabber.properties $pwd > /home/hdfs/otg/log/log.txt &

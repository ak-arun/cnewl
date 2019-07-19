# cnewl


===============================================================================================

hivequerygrabber - A hive hook to capture queries against hive and stream the queries to Kafka topic. 

This code need to be modified to adapt to all range of kafka installs. Current code works for SASL_SSL kafka configuration and kerberised hive. 

The follwing hive configurations are needed to operationalize the hook. 

set hivehook.kafka.security.protocol=SASL_SSL;

set hivehook.kafka.serviceName=hdf-kafka;

set hivehook.kafka.bootstrapServers=***

set hivehook.kafka.sslcontext.truststore.type=JKS;

set hivehook.kafka.sslcontext.truststore.password=***

set hivehook.kafka.sslcontext.truststore.file=***;

set hivehook.kafka.topicName=***; 

set hive.exec.pre.hooks=com.ak.hive.querygrabber.hook.QueryHook;

set hive.exec.post.hooks=com.ak.hive.querygrabber.hook.QueryHook;

set hive.exec.failure.hooks=com.ak.hive.querygrabber.hook.QueryHook;

The jar for this application need to be made available on the hive aux path. 

For hive cli to work, the jar need to be accessible to the hive clients. For HS2 connections (like beeline) the jar need only be present on the 
auxpath accessible locally by the hs2 servers. 

To create jar, use mvn clean package 

For RPM based distribution, run the rpm -ivh target/rpm/hivequerygrabber/RPMS/noarch/hivequerygrabber-0.0.1-1.noarch.rpm after building the 
mvn project and copy the jar from the install directory

For setting hive aux path, refer instructions at https://doc.lucidworks.com/fusion-server/4.0/search-development/getting-data-in/other-ingest-methods/import-via-hive.html#add-the-serde-jar-to-hive-classpath

===============================================================================================

onetimegrabber

This project captures the ddls for all tables in hive. It connects to the hive metastore database, gets the db and table listing and connects to hive to capture the actual ddl. Once the ddl is fetched, it pushes those into an rdbms. 
The program is controlled by a property file ( provided ) 

Build

mvn clean package

Running Instructions ( on an HDP cluster  )

java -cp onetimegrabber-0.0.1.jar:/usr/hdp/current/hive-client/lib/*:/usr/hdp/current/hadoop-client/lib/*:/usr/hdp/current/hadoop-client/* com.ak.hive.ddlgrabber.onetimegrabber.HiveDDLOnetimeGrabber grabber.properties






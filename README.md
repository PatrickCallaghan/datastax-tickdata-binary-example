Tick Data example
========================================================

This is a simple example of using C* as a tick data store for financial market data.

## Running the demo 

You will need a java runtime (preferably 7) along with maven 3 to run this demo. Start DSE 4.X or a cassandra 2.X instance on your local machine. This demo just runs as a standalone process on the localhost.

This demo uses quite a lot of memory so it is worth setting the MAVEN_OPTS to run maven with more memory

    export MAVEN_OPTS=-Xmx1024M

## Queries

The queries that we want to be able to run is 
	
1. Get all the tick data for a symbol in an exchange (in a time range)

     select * from tick_data where symbol ='NASDAQ-NFLX-2014-01-31';
     
to see what data is there.	

## Data 

The data is generated from a tick generator which uses a csv file to create random values from AMEX, NYSE and NASDAQ. The data will be down to the millisecond level.

## Throughput 

To increase the throughput, add nodes to the cluster. Cassandra will scale linearly with the amount of nodes in the cluster.

## Schema Setup
Note : This will drop the keyspace "datastax_tickdata_binary_demo" and create a new one. All existing data will be lost. 

The schema can be found in src/main/resources/cql/

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run the insert

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.tickdata.Main"

To read a ticker

	mvn clean compile exec:java -Dexec.mainClass="com.datastax.tickdata.Read" (-Dsymbol=NASDAQ-AAPL-2014-12-11)

To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
	

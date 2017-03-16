# Kafka Connect Cassandra Sink Connector (Experimental!)

kafka-connect-cassandra is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect) for loading data produced by Change Data Capture (CDC) sources into Cassandra.

This is experimental, currently unfinished and untested. Don't use it (yet). 

Motivation: explore CDC scenarios between RDBMS (for example Oracle) and NoSQL systems (for example Cassandra).

# TODO

- Add a blacklist of fields to ignore (i.e. op_* sort of fields...)
- Add configuration for INSERT, UPDATE, DELETE operations (i.e. op_type='I', etc.)
- Complete functionality to actually perform the selected operation on the Cassandra backend
- Testing, testing, testing, testing (and unit tests... but how to do this emulating Oracle or other DBs???)

# Development

To build a development version you'll need a recent version of Apache Kafka. 
You can build kafka-connect-cassandra with Maven using the standard lifecycle phases.

# Contribute

If you want to help out with development or testing:

```
git clone https://github.com/castagna/kafka-connect-cassandra.git
cd kafka-connect-cassandra
mvn package
cp -a target/kafka-connect-cassandra-0.1-SNAPSHOT-package/* $CONFLUENT_HOME/
```

- Source Code: https://github.com/castagna/kafka-connect-cassandra
- Issue Tracker: https://github.com/castagna/kafka-connect-cassandra/issues

# Credits

Greatly inspired by (i.e. mostly copied from):

- https://github.com/jcustenborder/kafka-connect-solr
- https://github.com/confluentinc/kafka-connect-jdbc

“Imitation is not just the sincerest form of flattery - it's the sincerest form of learning.” ― George Bernard Shaw

# License

The project is licensed under the Apache 2 license.


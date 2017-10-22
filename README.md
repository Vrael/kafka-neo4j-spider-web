## Zookeeper

Start zookeeper:
`./bin/zookeeper-server-start.sh config/zookeeper.properties`


## Kafka

### Create Kafka topics:

#### Links

`$ ./bin/kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 2 --topic links`

### Raw documents

`$ ./bin/kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 3 --topic documents`

### Parsed documents:

`$ ./bin/kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 2 --topic documents_parsed`


Remove topics:
`$ ./bin/kafktopics.sh --zookeeper localhost:2181 --delete --topic links`
`$ ./bin/kafktopics.sh --zookeeper localhost:2181 --delete --topic documents`
`$ ./bin/kafktopics.sh --zookeeper localhost:2181 --delete --topic documents_parsed`

### Feed links
`$ ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic links`

Example:
`{"location":"http://upm.es"}`


## Aplications:
* spider-downloader: Get locations from links kafka topic, download the raw html from url and send it to documents topic.
* spider-parser: Parse raw html from documents kafka topic, search links and produce a cooked document on document_parsed topic
* spider-repository: Save the links and relationship on neo4j graph database.


## Neo4j:
Start service: `sudo service neo4j`

Visualizer: `http://localhost:7474/browser/`

User: Neo4j
Pass: password

List all nodes:
`MATCH (p) RETURN p LIMIT 20`

Remove all nodes:
`MATCH (p) DETACH DELETE p`

Create node if not exists: 
`MERGE (o:Page{ url: "http://test.com/" }) ON CREATE SET o = {url: "http://test.com/"} ON MATCH SET o += {url: "http://test.com/"}`

Create link between two nodes if not exists:
`MERGE (o:Page{ url: "http://test.com/" }) MERGE (d:Page{ url: "http://prueba.com/" }) MERGE (o)-[:LINKS]->(d)`
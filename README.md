# spark-ddos-java

- Spark application written in Java for processing DDOS attacks
- Depends on upstream project https://github.com/RaghavendraBhat17/kafka-producer which is responsible for pushing records to topic

### Build artifact

```sh
gradle clean build
```
- The artifact will be generated under build/distributions folder

### Running application

- Untar the artifact from build/distributions folder
- Navigate to bin folder
```sh
./spark-ddos-java <ZK Quorum> <group id> <topic name>
Example: ./spark-ddos-java localhost:2181 1 test-topic
```

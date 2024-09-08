
This repository provides a local setup of Kafka for producing messages and a Flink instance. It includes a Flink job that consumes messages from Kafka, performs a max operation every 10 seconds, and sinks the results either to a file or another Kafka topic, depending on the sink class used (`org.example.fileSinker` or `org.example.kafkaSinker`).

#### **Read This First** 
##### All these are examples for references used in the initial setup in my macbook. Adjust them based on your local environment.

## Docker Compose Commands

### To Bring Up the Setup:
```bash
docker-compose -f localdatasetup.yml up -d
```

If there are any conflicts with the Docker image tag, remove the old image using:
```bash
docker rm <old_img_id>
```

Make the `localData` directory if it does not already exist:
```bash
mkdir Users/teaugene/localData
```

### To Bring Up the Setup / Change Only the Volume at Runtime:
```bash
docker-compose -f localdatasetup.yml up -d --no-deps --build jobmanager taskmanager1 taskmanager2 taskmanager3
```

### Stopping the Containers:
```bash
docker stop <containerID1> <ContainerId2>
```

### Docker Logs:
```bash
docker logs kafka -f
docker logs taskmanager1 -f
```

### Docker Compose Stop:
```bash
docker-compose -f localdatasetup.yml stop
```

---

# Local Kafka Producer & Consumer

In the `dataSetup` directory:

1. Clean and install the project:
   ```bash
   mvn clean install
   ```
2. Run the individual files from IntelliJ, or use the `java` command and run from the JAR if needed.

---

# Building the Flink Job JAR

In the `localflink` directory:

1. Clean and package the project:
   ```bash
   mvn clean package
   ```

2. From the Flink build repository, run the following commands:
   ```bash
   /Users/teaugene/github/flink/build-target/bin
   ./flink run -m localhost:8081 -c org.example.FileSinker /Users/teaugene/localFlink/localflink/target/flink-dummy-job-jar-with-dependencies.jar
   ./flink run -m localhost:8081 -c org.example.kafkaSinker /Users/teaugene/localFlink/localflink/target/flink-dummy-job-jar-with-dependencies.jar
   ```

You should receive something like this:
```
Job has been submitted with JobID f3275b16fa5ebd2d3b67e74fdef5a80c
```

Wait for a while to ensure the job is running.

- The File Sink should write to:
  `/Users/teaugene/localData/max_values/`
  - Check for hidden files in that directory.
  
- The Kafka Sink would write to Kafka topic: `your-topic3`.
  - Use the `dataSetup->kafkaConsumer` class to read the topic.

---


package edu.indiana.d2i.prov.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;


public class ProvKafkaProducer {

    private static ProvKafkaProducer provProducer;
    private Producer<String, String> kafkaProducer;
    //    private Producer<String, JsonNode> kafkaProducer;
    private static String kafkaTopic;
    private static String partitionStrategy;
    //    private static final String kafkaTopic = "mr-prov";
    private static Properties producerProperties;
    private static int numberOfPartitions;
    private static int partitionToWrite;
    public int messageCount = 0;

    private ProvKafkaProducer() {
        loadPropertiesFromFile();
        kafkaTopic = producerProperties.getProperty("kafka.topic");
        partitionStrategy = producerProperties.getProperty("partition.strategy");
        partitionToWrite = Integer.parseInt(producerProperties.getProperty("partition.to.write"));
        numberOfPartitions = Integer.parseInt(producerProperties.getProperty("number.of.partitions"));
        System.out.println("#### properties read, partition to write = " + partitionToWrite);

        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
        props.put("bootstrap.servers", producerProperties.getProperty("bootstrap.servers"));
//        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
        props.put("batch.size", 50000); // in bytes
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
        props.put("buffer.memory", 1000000000);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("key.serializer", "org.apache.kafka.connect.json.JsonConverter");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
//        props.put("value.serializer", "org.apache.kafka.connect.json.JsonConverter");

        kafkaProducer = new KafkaProducer<>(props);

    }

    public static int getNumberOfPartitions() {
        return numberOfPartitions;
    }

    public static int getPartitionToWrite() {
        return partitionToWrite;
    }

    public static String getPartitionStrategy() {
        return partitionStrategy;
    }

    private void loadPropertiesFromFile() {
        producerProperties = new Properties();
        try {
            producerProperties.load(new FileInputStream("/home/isurues/hadoop/kafka.properties"));
//            producerProperties.load(new FileInputStream("/Users/isuru/research/streaming-prov/kafka-producer/kafka.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized ProvKafkaProducer getInstance() {
        if (provProducer == null) {
            provProducer = new ProvKafkaProducer();
        }
        return provProducer;
    }

    public void close() {
        kafkaProducer.close();
    }

    public void createActivity(String id, String function) {
        String notification = "{\"id\":\"" + id +
                "\", \"nodeType\":\"ACTIVITY\", \"type\":\"node\", \"attributes\":{\"function\":\"" +
                function + "\"}}";
        kafkaProducer.send(new ProducerRecord<String, String>(kafkaTopic, "line", notification));
    }

    public void createEntity(String id, String key, String value) {
        String notification = "{\"id\":\"" + id +
                "\", \"nodeType\":\"ENTITY\", \"type\":\"node\", \"attributes\":{\"key\":\"" + key +
                "\", \"value\":\"" + value + "\"}}";
        kafkaProducer.send(new ProducerRecord<String, String>(kafkaTopic, "line", notification));
    }

    public void createEntity(String id) {
        kafkaProducer.send(new ProducerRecord<String, String>(kafkaTopic, "line",
                "{\"id\":\"" + id + "\", \"nodeType\":\"ENTITY\", \"type\":\"node\"}"));
    }

//    public void createEdge(String sourceId, String destId, String edgeType) {
//        String notification = "{\"sourceId\":\"" + sourceId + "\", \"destId\":\"" +
//                destId + "\", \"edgeType\":\"" + edgeType +
//                "\", \"type\":\"edge\"}";
//        kafkaProducer.send(new ProducerRecord<String, String>(kafkaTopic, "line", notification));
//    }

    public void createAndSendEdge(String sourceId, String destId, String edgeType, int partition) {
        String notification = "{\"sourceId\":\"" + sourceId + "\", \"destId\":\"" +
                destId + "\", \"edgeType\":\"" + edgeType + "\", \"partition\":\"" + partition + "\"}";
//        ObjectMapper objectMapper = new ObjectMapper();
//        JsonNode jsonNode = objectMapper.valueToTree(notification);
//        check(notification);
//        count();
        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, partition, "line", notification));
//        kafkaProducer.send(new ProducerRecord<String, JsonNode>(kafkaTopic, jsonNode));
    }

    public String createEdge(String sourceId, String destId, String edgeType, int partition) {
        return "{\"sourceId\":\"" + sourceId + "\", \"destId\":\"" +
                destId + "\", \"edgeType\":\"" + edgeType + "\", \"partition\":\"" + partition + "\"}";
    }

    public void createAndSendJSONArray(List<String> notifications, String edgeType, int partition) {
        if (notifications.size() == 1) {
//            check(notifications.get(0));
//            count();
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, partition, "line", notifications.get(0)));
        } else if (notifications.size() > 1) {
            StringBuilder array = new StringBuilder("{\"group\":[");
            for (int i = 0; i < notifications.size(); i++) {
                array.append(notifications.get(i));
                if (i < notifications.size() - 1)
                    array.append(", ");
            }
            array.append("], \"edgeType\":\"").append(edgeType)
                    .append("\", \"partition\":\"").append(partition).append("\"}");
            String notification = array.toString();
//            check(notification);
//            count();
            kafkaProducer.send(new ProducerRecord<>(kafkaTopic, partition, "line", notification));
        }
    }

//    private void count() {
//        if (++messageCount % 5000 == 0)
//            System.out.println("@@>>> message count = " + messageCount);
//    }

//    private void check(String notification) {
//        if (notification.contains("2811"))
//            System.out.println("###@@@@### 2811 notification = " + notification);
//    }

}

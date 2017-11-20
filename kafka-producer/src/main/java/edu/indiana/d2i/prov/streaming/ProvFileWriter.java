package edu.indiana.d2i.prov.streaming;

import java.io.*;
import java.util.List;
import java.util.Properties;


public class ProvFileWriter {

    private BufferedWriter writer = null;

    private static ProvFileWriter provFileWriter;
    private static String partitionStrategy;
    private static Properties producerProperties;
    private static int numberOfPartitions;
    private static int partitionToWrite;

    private ProvFileWriter() {
        loadPropertiesFromFile();
        partitionStrategy = producerProperties.getProperty("partition.strategy");
        partitionToWrite = Integer.parseInt(producerProperties.getProperty("partition.to.write"));
        numberOfPartitions = Integer.parseInt(producerProperties.getProperty("number.of.partitions"));

        try {
            File logFile = new File("/Users/isuru/Desktop/ccgrid-2018/experiments/mr-prov");
            writer = new BufferedWriter(new FileWriter(logFile, true));
        } catch (Exception e) {
            e.printStackTrace();
        }

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
//            producerProperties.load(new FileInputStream("/home/isurues/hadoop/kafka.properties"));
            producerProperties.load(new FileInputStream("/Users/isuru/research/streaming-prov/kafka-producer/kafka.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static synchronized ProvFileWriter getInstance() {
        if (provFileWriter == null) {
            provFileWriter = new ProvFileWriter();
        }
        return provFileWriter;
    }

    public void close() {
        try {
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createAndSendEdge(String sourceId, String destId, String edgeType, int partition) {
        String notification = "{\"sourceId\":\"" + sourceId + "\", \"destId\":\"" +
                destId + "\", \"edgeType\":\"" + edgeType + "\", \"partition\":\"" + partition + "\"}";
//        ObjectMapper objectMapper = new ObjectMapper();
//        JsonNode jsonNode = objectMapper.valueToTree(notification);
//        check(notification);
//        count();
//        kafkaProducer.send(new ProducerRecord<>(kafkaTopic, partition, "line", notification));
        writeNotification(notification);
//        kafkaProducer.send(new ProducerRecord<String, JsonNode>(kafkaTopic, jsonNode));
    }

    public String createEdge(String sourceId, String destId, String edgeType, int partition) {
        return "{\"sourceId\":\"" + sourceId + "\", \"destId\":\"" +
                destId + "\", \"edgeType\":\"" + edgeType + "\", \"partition\":\"" + partition + "\"}";
    }

    public void createAndSendJSONArray(List<String> notifications, String edgeType, int partition) {
        if (notifications.size() == 1) {
            writeNotification(notifications.get(0));
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
            writeNotification(notification);
        }
    }

    private void writeNotification(String notification) {
        try {
            writer.append(notification);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

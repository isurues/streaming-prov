package edu.indiana.d2i.flink.async;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AsyncProvStreamConsumerCount {

    public static Properties fileProps;
    static {
        fileProps = loadPropertiesFromFile();
        System.out.println("@@@ kafka properties loaded: " + fileProps.getProperty("bootstrap.servers"));
    }

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", fileProps.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", "local_consumer");

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(
                fileProps.getProperty("kafka.topic"), new JSONDeserializationSchema(), properties));

        DataStream<ObjectNode> filteredStream = stream.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode value) throws Exception {
                String edgeType = value.get("edgeType").asText();
                return "wasGeneratedBy".equals(edgeType) || "used".equals(edgeType);
            }
        });

        DataStream<Tuple2<String, ObjectNode>> keyedStream = filteredStream.map(
                new MapFunction<ObjectNode, Tuple2<String, ObjectNode>>() {
                    private static final long serialVersionUID = -6867736771747690202L;

                    @Override
                    public Tuple2<String, ObjectNode> map(ObjectNode value) throws Exception {
                        return new Tuple2<>(value.get("partition").asText(), value);
                    }
                });

        keyedStream
                .keyBy(0)
                .process(new AsyncLocalReducerCount())
                .writeAsText(fileProps.getProperty("output.file.path"));

        env.execute();
    }

    private static Properties loadPropertiesFromFile() {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("/home/isurues/flink/kafka.properties"));
//            properties.load(new FileInputStream("/Users/isuru/research/streaming-prov/flink-consumer/kafka.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }

}

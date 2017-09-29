package edu.indiana.d2i.flink;


import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class ProvStreamConsumerJson {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink_consumer");

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(
                "mr-prov", new JSONDeserializationSchema(), properties) );

        stream.map(new MapFunction<ObjectNode, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(ObjectNode value) throws Exception {
                return "Stream Value: " + value.get("sourceId") + "," + value.get("destId") + "," + value.get("edgeType");
//                return "Stream Value: " + value.toString();
            }
        }).print();

        env.execute();
    }

}

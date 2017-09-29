package edu.indiana.d2i.flink.inmemory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.queued.GlobalReducer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;

import java.util.Properties;

public class ProvStreamConsumer {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "prov_consumer");

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(
                "k10-mr-prov-p3", new JSONDeserializationSchema(), properties));

        DataStream<ObjectNode> filteredStream = stream.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode value) throws Exception {
                String edgeType = value.get("edgeType").asText();
                return "wasGeneratedBy".equals(edgeType) || "used".equals(edgeType);
            }
        });

        filteredStream
                .process(new LocalReducer())
                .process(new GlobalReducer()).setParallelism(1)
                .print();

        env.execute();
    }

}

package edu.indiana.d2i.flink.inmemory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.queued.GlobalReducer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;

import java.util.Properties;

public class ProvStreamConsumer {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "prov_consumer");

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer09<>(
                "mr-prov", new JSONDeserializationSchema(), properties));

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
                        return new Tuple2<>("mykey", value);
                    }
                });

        keyedStream
                .keyBy(0)
                .process(new LocalReducer())
                .keyBy(0)
                .process(new GlobalReducer())
                .print();

        env.execute();
    }

}

package edu.indiana.d2i.flink.keyed;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;

import java.util.Properties;

public class KeyedProvStreamConsumer {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "local_consumer");

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(
                "k10-mr-prov-p3", new JSONDeserializationSchema(), properties));

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
                .process(new KeyedLocalReducer())
                .keyBy(0)
                .process(new KeyedGlobalReducer()).setParallelism(1)
                .print();

        env.execute();
    }

}

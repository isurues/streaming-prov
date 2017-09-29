package edu.indiana.d2i.flink.queued;


import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.utils.ProvEdge;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;

import java.util.Properties;

public class GlobalProvStreamConsumer {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "global_consumer");

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(
                "mr-prov-reduced", new JSONDeserializationSchema(), properties));

        DataStream<Tuple2<String, ProvEdge>> keyedStream = stream.map(
                new MapFunction<ObjectNode, Tuple2<String, ProvEdge>>() {
                    private static final long serialVersionUID = -6867736771747690202L;

                    @Override
                    public Tuple2<String, ProvEdge> map(ObjectNode value) throws Exception {
                        return new Tuple2<>("mykey", new ProvEdge(value.get("sourceId").asText(), value.get("destId").asText()));
                    }
                });

        keyedStream
                .keyBy(0)
//                .process(new GlobalReducer())
                .print();

        env.execute();
    }

}

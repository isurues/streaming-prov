package edu.indiana.d2i.flink.test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class PartitionConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test_group");

//        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>("test-p3", new SimpleStringSchema(), properties));

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>("test-p3",
                new JSONDeserializationSchema(), properties));

//        DataStream<Tuple2<String, ObjectNode>> mappedStream = stream.map(
//                new MapFunction<ObjectNode, Tuple2<String, ObjectNode>>() {
//                    private static final long serialVersionUID = -6867736771747690202L;
//
//                    @Override
//                    public Tuple2<String, ObjectNode> map(ObjectNode value) throws Exception {
//                        return new Tuple2<>("mykey", value);
//                    }
//                });

//        DataStream<ObjectNode> mappedStream = stream.map(
//                new MapFunction<ObjectNode, ObjectNode>() {
//                    private static final long serialVersionUID = -6867736771747690202L;
//
//                    @Override
//                    public ObjectNode map(ObjectNode value) throws Exception {
//                        return value.put("mapped", "true");
//                    }
//                });

        stream
//                .keyBy(0).rebalance()
                .process(new OperatorStateReducer())
                .print();
//                .process(new PartitionReducer()).setParallelism(3);

        env.execute();
    }

}

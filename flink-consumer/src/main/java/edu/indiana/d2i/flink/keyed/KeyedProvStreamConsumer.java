package edu.indiana.d2i.flink.keyed;

import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.utils.ProvEdge;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KeyedProvStreamConsumer {

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
//        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", fileProps.getProperty("bootstrap.servers"));
        properties.setProperty("group.id", "local_consumer");
//        properties.setProperty("auto.offset.reset", "earliest");

        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(
                fileProps.getProperty("kafka.topic"), new JSONDeserializationSchema(), properties));
//                "mr-prov", new JSONDeserializationSchema(), properties));

        DataStream<ObjectNode> filteredStream = stream.filter(new FilterFunction<ObjectNode>() {
            @Override
            public boolean filter(ObjectNode value) throws Exception {
//                String stringValue = value.toString();
//                if (stringValue.contains("2811"))
//                    System.out.println("----> 2811 notification = " + stringValue);
                String edgeType = value.get("edgeType").asText();
                return "wasGeneratedBy".equals(edgeType) || "used".equals(edgeType);
            }
        });

        DataStream<Tuple2<String, ObjectNode>> keyedStream = filteredStream.map(new PartitionMapper());

//        DataStream<Tuple2<String, ObjectNode>> keyedStream = filteredStream.map(
//                new MapFunction<ObjectNode, Tuple2<String, ObjectNode>>() {
//                    private static final long serialVersionUID = -6867736771747690202L;
//
//                    @Override
//                    public Tuple2<String, ObjectNode> map(ObjectNode value) throws Exception {
//                        return new Tuple2<>(value.get("partition").asText(), value);
//                    }
//                });

//        BucketingSink<ProvEdge> sink = new BucketingSink<>(fileProps.getProperty("output.file.path"));
//        sink.setInactiveBucketCheckInterval(5000);
//        sink.setInactiveBucketThreshold(5000);

        keyedStream
                .keyBy(0)
                .process(new KeyedGroupLocalReducer())
//                .process(new KeyedLocalReducer())
                .keyBy(0)
                .process(new KeyedGroupGlobalReducer()).setParallelism(1)
//                .process(new KeyedGlobalReducer()).setParallelism(1)
                .writeAsText(fileProps.getProperty("output.file.path")).setParallelism(1);
//                .addSink(sink).setParallelism(1);

        // how to write to an hdfs file
        // .writeAsText("hdfs://nnHost:nnPort/flink-out/out1");

//                .print();

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

    private static class PartitionMapper extends RichMapFunction<ObjectNode, Tuple2<String, ObjectNode>> {

        private Meter meter;

        @Override
        public void open(Configuration parameters) throws Exception {
            com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();
            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("provMeter", new DropwizardMeterWrapper(meter));
        }

        @Override
        public Tuple2<String, ObjectNode> map(ObjectNode value) throws Exception {
            this.meter.markEvent();
            return new Tuple2<>(value.get("partition").asText(), value);
        }

    }

}

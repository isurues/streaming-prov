package edu.indiana.d2i.flink.count;


import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KeyedCounter {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink_consumer");

//        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer010<>(
//                "mr-prov", new JSONKeyValueDeserializationSchema(false), properties) );
//
//        DataStream<Tuple2<String, String>> tupleStream = stream.map(new MapFunction<ObjectNode, Tuple2<String, String>>() {
//            private static final long serialVersionUID = -6867736771747690202L;
//
//            @Override
//            public Tuple2<String, String> map(ObjectNode value) throws Exception {
//                return new Tuple2<>(value.get("edgeType").asText(), value.toString());
//            }
//        });

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(
                "mr-prov", new SimpleStringSchema(), properties) );

        DataStream<Tuple2<String, String>> tupleStream = stream.map(new MapFunction<String, Tuple2<String, String>>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("dummyKey", value);
            }
        });

        // apply the process function onto a keyed stream
        DataStream<Tuple2<String, Long>> result = tupleStream
                .keyBy(0)
                .process(new CountWithTimeoutFunction());

        env.execute();
    }

    /**
     * The data type stored in the state
     */
    public static class CountWithTimestamp {

        public String key;
        public long count;
        public long lastModified;
    }

    /**
     * The implementation of the ProcessFunction that maintains the count and timeouts
     */
    public static class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {

        /** The state that is maintained by this process function */
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {

            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // update the state's count
            current.count++;

            // set the state's timestamp to the record's assigned event time timestamp
//            current.lastModified = ctx.timestamp();

            // write the state back
            state.update(current);
            System.out.println("count = " + current.count);

            // schedule the next timer 60 seconds from the current event time
//            ctx.timerService().registerEventTimeTimer(current.lastModified + 5000);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
                throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();

            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 5000) {
                // emit the state on timeout
                out.collect(new Tuple2<String, Long>(result.key, result.count));
            }
        }
    }

}

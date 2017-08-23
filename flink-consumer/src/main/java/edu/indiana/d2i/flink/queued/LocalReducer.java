package edu.indiana.d2i.flink.queued;


import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.utils.ProvEdge;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class LocalReducer extends ProcessFunction<Tuple2<String, ObjectNode>, String> {

    private ValueState<ProvState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("localState", ProvState.class));
    }

    @Override
    public void processElement(Tuple2<String, ObjectNode> in, Context context,
                               Collector<String> out) throws Exception {

        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        current.count++;
        current.processNotification(in.f1);
        state.update(current);

//        current.lastModified = context.timestamp();

        if (current.count % 11 == 0) {
//            out.collect(new Tuple2<>(in.f0, current.count));
//            current.printState();

            for (String key : current.edgesBySource.keySet()) {
                List<ProvEdge> edges = current.edgesBySource.get(key);
                for (ProvEdge e : edges)
                    out.collect(e.toJSONString());
            }
//            out.collect(new Tuple2<>(in.f0, new ProvEdge("---", "---")));
            current.clearState();
        }

//        context.timerService().registerEventTimeTimer(current.lastModified + 5000);



    }

//    @Override
//    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out)
//            throws Exception {
//
//        System.out.println("onTimer invoked..");
//
//        // get the state for the key that scheduled the timer
//        ProvState result = state.value();
//
//        // check if this is an outdated timer or the latest timer
//        if (timestamp == result.lastModified + 5000) {
//            // emit the state on timeout
//            out.collect(new Tuple2<String, Long>(result.key, result.count));
//        }
//    }


}

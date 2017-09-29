package edu.indiana.d2i.flink.keyed;

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

public class KeyedLocalReducer extends ProcessFunction<Tuple2<String, ObjectNode>, Tuple2<String, ProvEdge>> {

    private ValueState<ProvState> state;
    private static final int LOCAL_LIMIT = 100;
    private static final int TIMER_INTERVAL_MS = 500;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("local-state", ProvState.class));
        System.out.println("local reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(Tuple2<String, ObjectNode> in, Context context,
                               Collector<Tuple2<String, ProvEdge>> out) throws Exception {
//        System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + ", " +
//                getRuntimeContext().getIndexOfThisSubtask() + " : " + in.f1.toString());

        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        current.count++;
        current.processNotification(in.f1);
        state.update(current);
        // emit on count, may be experiment with a time period too?
        if (current.count == LOCAL_LIMIT) {
            System.out.println(current.key + ": count: emitting local results...");
            emitState(current, out);
        }

        current.lastModified = System.currentTimeMillis();
        context.timerService().registerProcessingTimeTimer(current.lastModified + TIMER_INTERVAL_MS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, ProvEdge>> out)
            throws Exception {
        // get the state for the key that scheduled the timer
        ProvState current = state.value();
        // check if this is an outdated timer or the latest timer
        if (timestamp == current.lastModified + TIMER_INTERVAL_MS) {
            System.out.println(current.key + ": timer: emitting local results...");
            emitState(current, out);
        }
    }

    private void emitState(ProvState current, Collector<Tuple2<String, ProvEdge>> out) {
        for (String key : current.edgesBySource.keySet()) {
            List<ProvEdge> edges = current.edgesBySource.get(key);
            for (ProvEdge e : edges)
                out.collect(new Tuple2<>("global", e));
        }
        current.clearState();
    }

}

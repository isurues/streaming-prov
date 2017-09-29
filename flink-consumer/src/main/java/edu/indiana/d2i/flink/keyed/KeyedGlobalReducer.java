package edu.indiana.d2i.flink.keyed;

import edu.indiana.d2i.flink.utils.ProvEdge;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class KeyedGlobalReducer extends ProcessFunction<Tuple2<String, ProvEdge>, ProvEdge> {

    private ValueState<ProvState> state;
    private static final int TIMER_INTERVAL_MS = 1000;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("global-state", ProvState.class));
        System.out.println("global reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(Tuple2<String, ProvEdge> in, Context context,
                               Collector<ProvEdge> out) throws Exception {
//        System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + ", " +
//                getRuntimeContext().getIndexOfThisSubtask() + ", " + in.f0 + " : " + in.f1.toString());

        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        current.count++;
        current.handleNewEdge(in.f1);
        state.update(current);

        current.lastModified = System.currentTimeMillis();
        context.timerService().registerProcessingTimeTimer(current.lastModified + TIMER_INTERVAL_MS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ProvEdge> out)
            throws Exception {

        // get the state for the key that scheduled the timer
        ProvState current = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == current.lastModified + TIMER_INTERVAL_MS) {
            System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ": emitting global results...");
            for (String key : current.edgesBySource.keySet()) {
                List<ProvEdge> edges = current.edgesBySource.get(key);
                for (ProvEdge e : edges)
                    out.collect(e);
            }
        }
    }

}

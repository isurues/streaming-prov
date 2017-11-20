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

public class KeyedGroupGlobalReducer extends ProcessFunction<Tuple2<String, List<ProvEdge>>, ProvEdge> {

    private ValueState<ProvState> state;
    private static final int TIMER_INTERVAL_MS = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("global.timer.interval"));

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("global-state", ProvState.class));
        System.out.println("@@@ global timer interval = " + TIMER_INTERVAL_MS);
        System.out.println("global reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(Tuple2<String, List<ProvEdge>> in, Context context,
                               Collector<ProvEdge> out) throws Exception {
        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        current.count++;
        current.handleNewEdgeGroup(in.f1);
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
                for (ProvEdge e : edges) {
                    String source = e.getSource();
                    if (source.startsWith("task_") && source.contains("_m_"))
                        continue;
//                    if (e.toString().contains("2811"))
//                        System.out.println(getRuntimeContext().getIndexOfThisSubtask() + ":2811 global ---> " + e.toJSONString());
                    out.collect(e);
                }
            }
        }
    }

}

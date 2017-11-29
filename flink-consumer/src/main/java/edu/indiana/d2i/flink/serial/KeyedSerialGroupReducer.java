package edu.indiana.d2i.flink.serial;

import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.keyed.KeyedProvStreamConsumer;
import edu.indiana.d2i.flink.utils.ProvEdge;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class KeyedSerialGroupReducer extends ProcessFunction<Tuple2<String, ObjectNode>, ProvEdge> {

    private ValueState<ProvState> state;
    private static final int TIMER_INTERVAL_MS = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("local.timer.interval"));

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("local-state", ProvState.class));
        System.out.println("@@@ local timer interval = " + TIMER_INTERVAL_MS);
        System.out.println("grouped local reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(Tuple2<String, ObjectNode> in, Context context, Collector<ProvEdge> out) throws Exception {
        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        current.count++;
        current.processNotification(in.f1);
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
            System.out.println(current.key + ": timer: emitting grouped local results...");
            for (String key : current.edgesBySource.keySet()) {
                List<ProvEdge> edges = current.edgesBySource.get(key);
                for (ProvEdge e : edges) {
                    String source = e.getSource();
                    if (source.startsWith("task_") && source.contains("_m_"))
                        continue;
                    out.collect(e);
                }
            }
        }
    }

}

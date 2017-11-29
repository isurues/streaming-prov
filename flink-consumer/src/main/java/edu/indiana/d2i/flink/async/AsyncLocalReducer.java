package edu.indiana.d2i.flink.async;

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
import java.util.Map;

public class AsyncLocalReducer extends ProcessFunction<Tuple2<String, ObjectNode>, Tuple2<String, List<ProvEdge>>> {

    private ValueState<ProvState> state;
    private static final int LOCAL_LIMIT = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("local.limit"));
    private static final int TIMER_INTERVAL_MS = Integer.parseInt(KeyedProvStreamConsumer.fileProps.getProperty("local.timer.interval"));

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("local-state", ProvState.class));
        System.out.println("@@@ local limit = " + LOCAL_LIMIT);
        System.out.println("@@@ local timer interval = " + TIMER_INTERVAL_MS);
        System.out.println("grouped local reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(Tuple2<String, ObjectNode> in, Context context,
                               Collector<Tuple2<String, List<ProvEdge>>> out) throws Exception {
        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        if (!current.started) {
            current.startMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            current.startTime = System.currentTimeMillis();
            current.started = true;
        }
        current.count++;
        current.processNotification(in.f1);
        current.numBytes += in.f1.toString().getBytes().length;
        state.update(current);

        if (current.count % 500 == 0) {
            long nowMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            long nowMemFootprint = nowMemory - current.startMemory;
            if (nowMemFootprint > current.maxMemFootprint)
                current.maxMemFootprint = nowMemFootprint;
        }

        // emit on count, may be experiment with a time period too?
        if (current.count == LOCAL_LIMIT) {
//            System.out.println(current.key + ": count: emitting grouped local results...");
            emitGroupedState(current, out, false);
        }

        current.lastModified = System.currentTimeMillis();
        context.timerService().registerProcessingTimeTimer(current.lastModified + TIMER_INTERVAL_MS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, List<ProvEdge>>> out)
            throws Exception {
        // get the state for the key that scheduled the timer
        ProvState current = state.value();
        // check if this is an outdated timer or the latest timer
        if (timestamp == current.lastModified + TIMER_INTERVAL_MS) {
//            System.out.println(current.key + ": timer: emitting grouped local results...");
            long time = current.lastModified - current.startTime;
            float mb = (float) current.numBytes / 1000000;
            float throughput = (float) current.numBytes / (1000 * time);
            System.out.println(current.key + ": Throughput = " + throughput +
                    "MB/s, Size = " + mb + "MB, Time = " + time + "ms");

            emitGroupedState(current, out, true);
//            System.out.println(current.key + ": timer: edges = " + current.edgeCount + " filtered = " + current.filteredEdgeCount);
        }
    }

    private void emitGroupedState(ProvState current, Collector<Tuple2<String, List<ProvEdge>>> out, boolean onTimer) {
//        for (String key : current.edgesBySource.keySet()) {
//            List<ProvEdge> edges = current.edgesBySource.get(key);
//            if (key.startsWith("task_") && key.contains("_m_")) {
//                current.filteredEdgeCount += edges.size();
//                continue;
//            }
//            current.edgeCount += edges.size();
//            out.collect(new Tuple2<>("global", edges));
//        }

        float maxMemFootprint = current.maxMemFootprint / 1000000;
        String mode = onTimer ? "timer" : "count";
        System.out.println(current.key + ": " + mode + " : Max memory footprint = " + maxMemFootprint);
        Thread t = new Thread(new Emitter(current.edgesBySource, out, current, onTimer));
        t.start();
        current.clearState();
    }


    private class Emitter implements Runnable {
        Map<String, List<ProvEdge>> edgesBySource;
        Collector<Tuple2<String, List<ProvEdge>>> out;
        ProvState state;
        boolean onTimer;

        Emitter(Map<String, List<ProvEdge>> edgesBySource, Collector<Tuple2<String,
                List<ProvEdge>>> out, ProvState current, boolean onTimer) {
            this.edgesBySource = edgesBySource;
            this.out = out;
            this.state = current;
            this.onTimer = onTimer;
        }

        @Override
        public void run() {
            int filteredEdgeCount = 0;
            int emittedEdgeCount = 0;
            for (String key : edgesBySource.keySet()) {
                List<ProvEdge> edges = edgesBySource.get(key);
                if (key.startsWith("task_") && key.contains("_m_")) {
                    filteredEdgeCount += edges.size();
                    continue;
                }
                emittedEdgeCount += edges.size();
                out.collect(new Tuple2<>("global", edges));
            }
            state.filteredEdgeCount += filteredEdgeCount;
            state.edgeCount += emittedEdgeCount;
            if (onTimer) {
                long totalTime = System.currentTimeMillis() - state.startTime;
                System.out.println(state.key + ": Emitter Thread: edges = " + state.edgeCount +
                        " filtered = " + state.filteredEdgeCount + " total time (ms) = " + totalTime);
            }
        }
    }


}

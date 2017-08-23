package edu.indiana.d2i.flink.queued;


import edu.indiana.d2i.flink.utils.ProvEdge;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class GlobalReducer extends ProcessFunction<Tuple2<String, ProvEdge>, Tuple2<String, ProvEdge>> {

    private ValueState<ProvState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("globalState", ProvState.class));
    }

    @Override
    public void processElement(Tuple2<String, ProvEdge> in, Context context,
                               Collector<Tuple2<String, ProvEdge>> out) throws Exception {

        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        current.count++;
        current.handleNewEdge(in.f1);
        state.update(current);

        if (current.count % 7 == 0) {
            for (String key : current.edgesBySource.keySet()) {
                List<ProvEdge> edges = current.edgesBySource.get(key);
                for (ProvEdge e : edges)
                    out.collect(new Tuple2<>(in.f0, e));
            }
            out.collect(new Tuple2<>(in.f0, new ProvEdge("---", "---")));
        }

    }
}

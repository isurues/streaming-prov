package edu.indiana.d2i.flink.inmemory;

import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.utils.ProvEdge;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class LocalReducer
        extends ProcessFunction<ObjectNode, ProvEdge>
        implements CheckpointedFunction {

    private transient ListState<ProvState> listState;

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        listState.clear();
        // TODO
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<ProvState> descriptor =
                new ListStateDescriptor<>("local-state",
                        TypeInformation.of(new TypeHint<ProvState>() {}));

        listState = context.getOperatorStateStore().getListState(descriptor);
        listState.add(new ProvState());

        // TODO
//        if (context.isRestored()) {
//        }

        System.out.println("local operator state initialized. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("local reducer open. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(ObjectNode in, Context context,
                               Collector<ProvEdge> out) throws Exception {
        System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + ", " + getRuntimeContext().getIndexOfThisSubtask() + " : " + in.toString());

        ProvState current = listState.get().iterator().next();
        current.count++;
        current.lastModified = System.currentTimeMillis();
        current.processNotification(in);

//        if (current.count % 11 == 0) {
//            for (String key : current.edgesBySource.keySet()) {
//                List<ProvEdge> edges = current.edgesBySource.get(key);
//                for (ProvEdge e : edges)
//                    out.collect(new Tuple2<>(in.f0, e));
//            }
//            current.clearState();
//        }

        context.timerService().registerProcessingTimeTimer(current.lastModified + 500);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ProvEdge> out)
            throws Exception {

        // get the state for the key that scheduled the timer
        ProvState current = listState.get().iterator().next();

        // check if this is an outdated timer or the latest timer
        if (timestamp == current.lastModified + 500) {
            System.out.println("emitting local results...");
            for (String key : current.edgesBySource.keySet()) {
                List<ProvEdge> edges = current.edgesBySource.get(key);
                for (ProvEdge e : edges) {
                    out.collect(e);
                }
            }
            current.clearState();
        }
    }


}

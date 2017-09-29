package edu.indiana.d2i.flink.test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class OperatorStateReducer
        extends ProcessFunction<ObjectNode, String>
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
                new ListStateDescriptor<>("reduced-listState",
                        TypeInformation.of(new TypeHint<ProvState>() {}));

        listState = context.getOperatorStateStore().getListState(descriptor);
        listState.add(new ProvState());

        // TODO
//        if (context.isRestored()) {
//        }

        System.out.println("operator state initialized. task = " + getRuntimeContext().getTaskNameWithSubtasks());
    }

    @Override
    public void processElement(ObjectNode in, Context context, Collector<String> out) throws Exception {
        System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + ", " + getRuntimeContext().getIndexOfThisSubtask() + " : " + in.toString());

        ProvState state = listState.get().iterator().next();
        state.count++;
        state.lastModified = System.currentTimeMillis();

        out.collect("processed: " + state.count);
    }
}

package edu.indiana.d2i.flink.test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.indiana.d2i.flink.utils.ProvState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class PartitionReducer extends ProcessFunction<Tuple2<String, ObjectNode>, String> {

    private ValueState<ProvState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("local reducer open. task = " + getRuntimeContext().getTaskName() +
                ", task with sub = " + getRuntimeContext().getTaskNameWithSubtasks());
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("localState", ProvState.class));
    }

    @Override
    public void processElement(Tuple2<String, ObjectNode> in, Context context,
                               Collector<String> out) throws Exception {
        System.out.println(getRuntimeContext().getTaskNameWithSubtasks() + ", " + getRuntimeContext().getIndexOfThisSubtask() + " : " + in.f1.toString());

        ProvState current = state.value();
        if (current == null) {
            current = new ProvState();
            current.key = in.f0;
        }

        current.count++;
        current.lastModified = System.currentTimeMillis();
        state.update(current);


        out.collect("processed: " + current.count);
    }

}

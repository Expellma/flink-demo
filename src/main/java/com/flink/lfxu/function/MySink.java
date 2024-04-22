package com.flink.lfxu.function;

import com.flink.lfxu.model.Event;
import com.flink.lfxu.utils.MyWriter;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

public class MySink<IN> extends RichSinkFunction<IN> {
    private final MyWriter<IN> writer;

    public MySink() {
        this.writer = new MyWriter<>(false);
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        StreamingRuntimeContext context = (StreamingRuntimeContext)this.getRuntimeContext();
        this.writer.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks(),context);
    }

    public void invoke(IN record) {
        this.writer.write(record);
    }

}

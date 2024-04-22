package com.flink.lfxu.function;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class DelayFunction extends ScalarFunction {
    private  transient Histogram latency;
    @Override
    public void open(FunctionContext context) {
    latency =
        context.getMetricGroup().histogram("latency-time", new DescriptiveStatisticsHistogram(100));
    }

    public long eval(Timestamp time) {
        long l = System.currentTimeMillis();
        latency.update(l-time.getTime());
        return l - time.getTime();
    }
}

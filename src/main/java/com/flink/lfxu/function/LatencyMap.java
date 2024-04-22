package com.flink.lfxu.function;

import com.flink.lfxu.model.Event;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

public class LatencyMap extends RichMapFunction<Event,Event>{
    private  transient Histogram latency;
    private long curCount;


    @Override
    public void open(Configuration parameters) throws Exception {
        latency =
                getRuntimeContext().getMetricGroup().histogram("latency-time", new DescriptiveStatisticsHistogram(30));
        curCount = 0L;
    }

    @Override
    public Event map(Event record) throws Exception {
        long eventTime = ((Event)record).getStartTime();
        latency.update( System.currentTimeMillis() - eventTime);
        return record;
    }
}

package com.flink.lfxu.utils;

import com.flink.lfxu.model.Event;
import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.PrintStream;
import java.io.Serializable;

public class MyWriter<IN> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final boolean STD_OUT = false;
    private static final boolean STD_ERR = true;

    private  transient Histogram latency;
    private final boolean target;
    private transient PrintStream stream;
    private final String sinkIdentifier;
    private transient String completedPrefix;
    public MyWriter() {
        this("", STD_OUT);
    }

    public MyWriter(final boolean stdErr) {
        this("", stdErr);
    }

    public MyWriter(final String sinkIdentifier, final boolean stdErr) {
        this.target = stdErr;
        this.sinkIdentifier = (sinkIdentifier == null ? "" : sinkIdentifier);
    }

    public void write(IN record) {
        long eventTime = ((Event)record).getStartTime();
        stream.println(completedPrefix + record.toString());
        latency.update( eventTime);
    }

    public void open(int subtaskIndex, int numParallelSubtasks, StreamingRuntimeContext context) {
        // get the target stream
        stream = target == STD_OUT ? System.out : System.err;

        completedPrefix = sinkIdentifier;

        if (numParallelSubtasks > 1) {
            if (!completedPrefix.isEmpty()) {
                completedPrefix += ":";
            }
            completedPrefix += (subtaskIndex + 1);
        }

        if (!completedPrefix.isEmpty()) {
            completedPrefix += "> ";
        }
        latency =
                context.getMetricGroup().histogram("latency-time", new DescriptiveStatisticsHistogram(30));
    }
    @Override
    public String toString() {
        return "Print to " + (target == STD_OUT ? "System.out" : "System.err");}
}

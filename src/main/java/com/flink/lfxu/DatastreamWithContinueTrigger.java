package com.flink.lfxu;

import com.flink.lfxu.Source.FileMockSource;
import com.flink.lfxu.function.AllKeyWindowsFunction;
import com.flink.lfxu.function.CustomWindowFunction;
import com.flink.lfxu.model.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

public class DatastreamWithContinueTrigger {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameters);
        DataStream<Event> dataStream = env.addSource(new FileMockSource()).setParallelism(1);

        dataStream
                .keyBy((KeySelector<Event, String>) Event::getMyKey)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                .process(new CustomWindowFunction())
                .print();

        env.execute("WindowWordCount");
    }
}

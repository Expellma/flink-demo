package com.flink.lfxu;

import com.flink.lfxu.Source.FileMockSource;
import com.flink.lfxu.function.AccumulateFunction;
import com.flink.lfxu.function.AllKeyWindowsFunction;
import com.flink.lfxu.function.CustomWindowFunction;
import com.flink.lfxu.model.Event;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DatastreamDemo2 {
  public static void main(String[] args) throws Exception {
    final ParameterTool parameters = ParameterTool.fromArgs(args);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(parameters);
    DataStream<Event> dataStream =
        env.addSource(new FileMockSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)));

    dataStream
        .keyBy((KeySelector<Event, String>) Event::getMyKey)
        .process(new AccumulateFunction())
        .print();

    env.execute("WindowWordCount");
  }

}

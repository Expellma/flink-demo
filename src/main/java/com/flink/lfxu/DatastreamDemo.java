package com.flink.lfxu;

import com.flink.lfxu.Source.FileMockSource;
import com.flink.lfxu.function.AllKeyWindowsFunction;
import com.flink.lfxu.function.CustomWindowFunction;
import com.flink.lfxu.function.LatencyMap;
import com.flink.lfxu.function.MySink;
import com.flink.lfxu.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class DatastreamDemo {
  public static void main(String[] args) throws Exception {
    final ParameterTool parameters = ParameterTool.fromArgs(args);
    Configuration configuration = new Configuration();
    configuration.setInteger(RestOptions.PORT, 8081);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    env.setParallelism(1);
    env.getConfig().setGlobalJobParameters(parameters);
    DataStream<Event> dataStream =
        env.addSource(new FileMockSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofHours(1)));

    dataStream
        .keyBy((KeySelector<Event, String>) Event::getMyKey)
        .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(0)))
        .process(new CustomWindowFunction())
        .keyBy(e -> "0")
        .process(new AllKeyWindowsFunction())
            .map(new LatencyMap())
            .print();

    env.execute("WindowWordCount");
  }

}

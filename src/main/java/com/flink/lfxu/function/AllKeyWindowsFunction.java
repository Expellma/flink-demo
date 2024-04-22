package com.flink.lfxu.function;

import com.flink.lfxu.model.Event;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class AllKeyWindowsFunction extends KeyedProcessFunction<String, Event, Event> {
  private String[] keyArr = new String[] {"key1", "key2", "key3", "key4"};

  private transient MapState<String, Integer> sumState;

  @Override
  public void open(Configuration configuration) {
    this.sumState =
        this.getRuntimeContext()
            .getMapState(new MapStateDescriptor<>("sumState", String.class, Integer.class));
  }

  @Override
  public void processElement(
      Event event,
      KeyedProcessFunction<String, Event, Event>.Context context,
      Collector<Event> collector)
      throws Exception {
    Integer val = this.sumState.get(context.getCurrentKey());
    if (val == null) val = 0;
    this.sumState.put(event.getMyKey(), val + event.getCount());
    context
        .timerService()
        .registerEventTimeTimer(
            getWindowStartWithOffset(context.timestamp(), 0,  5*1000) + 5*1000);
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out) throws Exception {
    // 输出此期间所有key的值
    for (String key : keyArr) {
      out.collect(
          new Event(
              key,
              timestamp,
              null,
              this.sumState.get(key) == null ? 0 : this.sumState.get(key)));
    }
    ;

    //        sumState.put(ctx.getCurrentKey(),0);
    ctx.timerService().registerEventTimeTimer(timestamp + 5* 1000);
  }

  private long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
    final long remainder = (timestamp - offset) % windowSize;
    // handle both positive and negative cases
    if (remainder < 0) {
      return timestamp - (remainder + windowSize);
    } else {
      return timestamp - remainder;
    }
  }
}

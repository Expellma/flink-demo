package com.flink.lfxu.function;

import com.flink.lfxu.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class AccumulateFunction extends KeyedProcessFunction<String, Event, Event> {
  private transient ValueState<Integer> sumState;

  @Override
  public void open(Configuration configuration) {
    this.sumState =
        this.getRuntimeContext().getState(new ValueStateDescriptor<>("sumState", Integer.class));
  }

  @Override
  public void processElement(
      Event event,
      KeyedProcessFunction<String, Event, Event>.Context context,
      Collector<Event> collector)
      throws Exception {
    if (sumState.value() == null) {
      sumState.update(0);
    }
    Integer curVal = sumState.value();
    Integer afterVal = curVal + event.getCount();
    log.info("key:{}, val:{}", context.getCurrentKey(), afterVal);
    sumState.update(afterVal);
  }
}

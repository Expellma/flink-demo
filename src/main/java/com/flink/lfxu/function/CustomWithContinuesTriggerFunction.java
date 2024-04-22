package com.flink.lfxu.function;

import com.flink.lfxu.model.Event;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CustomWithContinuesTriggerFunction
    extends ProcessWindowFunction<Event, Event, String, TimeWindow> {
    private String[] keyArr;
    private transient MapState<String, Integer> sumState;
    private transient MapState<String,Long> collectState;
    public CustomWithContinuesTriggerFunction(){
        this.keyArr = new String[] {"key1", "key2", "key3", "key4"};
    }

    @Override
    public void open(Configuration configuration) {
        this.sumState =
                this.getRuntimeContext()
                        .getMapState(new MapStateDescriptor<>("sumState", String.class, Integer.class));
        this.collectState =
                this.getRuntimeContext()
                        .getMapState(new MapStateDescriptor<>("collectState", String.class, Long.class));
    }


  @Override
  public void process(
      String curKey,
      ProcessWindowFunction<Event, Event, String, TimeWindow>.Context context,
      Iterable<Event> iterable,
      Collector<Event> collector)
      throws Exception {
        if(sumState.get(curKey) == null ) {
            collectState.put(curKey,0L);
            sumState.put(curKey,0);
        }
        Integer sum = sumState.get(curKey);
        for (Event event : iterable) {
            sum += event.getCount();
        }

  }

}

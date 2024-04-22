package com.flink.lfxu.function;

import com.flink.lfxu.model.Event;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;


public class CustomWindowFunction extends ProcessWindowFunction<Event, Event, String, TimeWindow> {
    private String[] keyArr;
    private transient MapState<String, Integer> sumState2;

    public CustomWindowFunction() {
        this.keyArr = new String[]{"key1", "key2", "key3", "key4"};
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.sumState2 = this.getRuntimeContext().getMapState(
                new MapStateDescriptor<>("sumState2", String.class, Integer.class));

    }

    @Override
    public void process(String s, Context context, Iterable<Event> iterable, Collector<Event> collector) throws Exception {
        long start = context.window().getStart();
        Integer cur = this.sumState2.get(s);
        if(cur == null) {
            cur = new Integer(0);
        };
        int sum = 0;
        Iterator<Event> iterator = iterable.iterator();
        while(iterator.hasNext()){
            sum += (Integer)iterator.next().getCount();
        }
        sum += cur;
        sumState2.put(s,sum);

        collector.collect(new Event(s,start,null,sum));
    }
}

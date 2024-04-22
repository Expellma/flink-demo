package com.flink.lfxu.Source;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.flink.lfxu.model.Event;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

public class FileMockSource extends RichSourceFunction<Event> {
  private volatile Boolean isRunning = true;

  @Override
  public void run(SourceContext<Event> sourceContext) throws Exception {
    String path = this.getClass().getClassLoader().getResource("").getPath();
//    System.out.println(path);
    String filePath = path + "data.txt";
    BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
    long offset = 5000L;
    int epoch = 0;
    while (isRunning) {
      String line = bufferedReader.readLine();
      if(line == null){
        bufferedReader = new BufferedReader(new FileReader(filePath));
        epoch +=1;
        Thread.sleep(10);
        continue;
      }
//      long eventTime = System.currentTimeMillis();
      String[] arr = line.split("\\|");
      long eventTime = Long.valueOf(arr[2]) + offset * epoch;
      Event event = new Event(arr[0], null, eventTime, Integer.valueOf(arr[1]));
      sourceContext.collectWithTimestamp(event,eventTime);
      sourceContext.emitWatermark(new Watermark(eventTime));

    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}

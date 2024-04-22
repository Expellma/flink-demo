package com.flink.lfxu;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TestClass {
    public static  AtomicBoolean flag = new AtomicBoolean(false);
  public static void main(String[] args) throws IOException, InterruptedException {
    //    System.out.println(new BigDecimal(Double.NaN).setScale(4, BigDecimal.ROUND_UP));
    //    System.out.println(new BigDecimal(new Double("0.000001")).setScale(4,
    // BigDecimal.ROUND_UP));
    //    System.out.println(new BigDecimal(new Double("0.000000000000001")).setScale(4,
    // BigDecimal.ROUND_UP));


      String tes = "|";
      byte[] bytes1 = tes.getBytes(StandardCharsets.UTF_8);

      log.info(String.valueOf(bytes1.length));

    ExecutorService executorService =
        Executors.newFixedThreadPool(2);
    int count =0;
    List<CompletableFuture> list = new ArrayList<>();
    while (count++<10){
        CompletableFuture future = CompletableFuture.runAsync(() ->{
            doThrow();
        },executorService).exceptionally(e -> {
            log.error(e.getMessage());
            executorService.shutdown();
            throw new RuntimeException(e);
        });
        list.add(future);
        Thread.sleep(2000);
    }
    try{
        list.forEach(res -> res.whenComplete((r,exception) -> {
            if (exception != null) {
                log.info("throe error");
                throw new RuntimeException((Exception)exception);
            }
        }));
    }catch (Exception e) {
      log.info("get exeception");
      executorService.shutdownNow();
      throw new RuntimeException(e);
    }















    String testStr = "dfsd_f212|da+f| ";
    byte bytes = ' ';
    Map<String,String> map1 = new HashMap<>();
    map1.put("1","1");
    HashMap<String, String> stringStringHashMap = new HashMap<>(map1);
    map1.clear();
    System.out.println("map1 "+map1.size());
    System.out.println(" map2 " + stringStringHashMap.size());

    System.out.println(bytes);
    for(String s: testStr.split("\\|")){
      System.out.println("s="+s);
    }
    long[] cases = new long[]{1390460419000L,1390460219000L,1390460400000L,1390460401000L,1709794799000L};
    for(long each: cases){
      System.out.println(each);
      isAfter(each);
    }
    System.out.println(1390460400000L/1000L % 86400L / 3600L);
  }

  private static void isAfter(long timestamp){
    // 将时间戳转换为 LocalDateTime
    Instant instant = Instant.ofEpochMilli(timestamp);
    LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

    // 获取当天的15:00时间点
    LocalTime fifteenPM = LocalTime.of(15, 0);

    // 判断时间是否在当天的15:00之后
    if (dateTime.toLocalTime().isAfter(fifteenPM)) {
      System.out.println("时间"+dateTime.toLocalTime()+"在当天15:00之后");
    } else {
      System.out.println("时间"+dateTime.toLocalTime()+"在当天15:00之前");
    }
    if (dateTime.toLocalTime().isAfter(LocalTime.of(11, 30)) && dateTime.toLocalTime().isBefore(LocalTime.of(13, 00))) {
      System.out.println("时间"+dateTime.toLocalTime()+"在当天11:00之后 13:00之前");
    } else {
      System.out.println("时间"+dateTime.toLocalTime()+"不  在当天11:00之后 13:00之前");
    }
  }


  private static void doThrow() {
      try{
          int ans = 19/0;
      }catch (Exception e ) {
          log.info(e.getMessage());
          throw new RuntimeException(e);
      }
  }
}
/*
1390460419000
时间10:14:20.419在当天15:00之前
时间10:14:20.419不  在当天11:00之后 13:00之前
1390460219000
时间10:14:20.219在当天15:00之前
时间10:14:20.219不  在当天11:00之后 13:00之前
1390460400000
时间10:14:20.400在当天15:00之前
时间10:14:20.400不  在当天11:00之后 13:00之前
1390460401000
时间10:14:20.401在当天15:00之前
时间10:14:20.401不  在当天11:00之后 13:00之前
 */
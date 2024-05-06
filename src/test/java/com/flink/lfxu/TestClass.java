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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class TestClass {
    public static  AtomicBoolean flag = new AtomicBoolean(false);

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

package com.flink.lfxu;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class UDTFdemo {
  public static final String CREATE_HANDLER_TIMESTRAMP_UDF_SQL = "create function if not exists latency as 'com.flink.lfxu.function.DelayFunction'";

  public static void main(String[] args) {
    Configuration configuration = new Configuration();
    configuration.setInteger(RestOptions.PORT, 8081);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    env.disableOperatorChaining();
    env.setParallelism(2);
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
    tableEnv.executeSql(CREATE_HANDLER_TIMESTRAMP_UDF_SQL);
    tableEnv.executeSql();


    tableEnv.executeSql();
    tableEnv.executeSql();
    tableEnv.executeSql();
  }

}
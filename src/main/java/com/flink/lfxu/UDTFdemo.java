package com.flink.lfxu;

import com.flink.lfxu.utils.SQLParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;
@Slf4j
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
    List<String> strings = SQLParser.getSqlFromFile("DemoUDTF.sql");
    strings.stream().forEach(e -> tableEnv.executeSql(e));
  }

}
package com.flink.lfxu;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
@Slf4j
public class WindowAgg {
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
    tableEnv.executeSql(
        "CREATE TABLE datagen (\n"
            + " f_sequence INT,\n"
            + " sec_code INT," +
                "test_number DECIMAL(6,3),\n"
            + " f_random_str STRING,\n"
            + " price INT,\n"
            + " ts AS localtimestamp,\n"
            + " WATERMARK FOR ts AS ts\n"
            + ") WITH (\n"
            + " 'connector' = 'datagen',\n"
            + " 'rows-per-second'='1000',\n"
            + " 'fields.f_sequence.kind'='sequence',\n"
            + " 'fields.f_sequence.start'='1',\n"
            + " 'fields.f_sequence.end'='1000000',\n"
            + " 'fields.sec_code.min'='100000',\n"
            + " 'fields.sec_code.max'='100100'," +
                "'fields.test_number.kind'='random',\n"
            + " 'fields.f_random_str.length'='4',\n"
            + " 'fields.price.min'='1',\n"
            + " 'fields.price.max'='10'\n"
            + ")");


    tableEnv.executeSql(
            "CREATE VIEW temp_view as select  \n"
                    + " sec_code," +
                    "  TUMBLE_END(ts, INTERVAL '2' SECOND) AS window_end,\n" +
                    " sum(price)," +
                    "count(1) \n"+
                    " from datagen " +
                    "group by sec_code,TUMBLE(ts,INTERVAL '2' SECOND)\n");
    tableEnv.executeSql(
        "CREATE TABLE print_table (\n"
            + " sec_code INT," +
                "end_timestamp TIMESTAMP," +
                " sum_price INT,\n" +
                "cnt BIGINT"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = 'test-agg',\n"
            + "  'properties.bootstrap.servers' = 'localhost:9092',\n"
            + "  'properties.group.id' = 'testGroup',\n"
            + "  'scan.startup.mode' = 'earliest-offset',\n"
            + "  'format' = 'csv'"
            + ")");
    tableEnv.executeSql("insert into print_table select " +
            "*" +
            "from temp_view");
  }

}
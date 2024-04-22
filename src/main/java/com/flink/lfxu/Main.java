package com.flink.lfxu;

import com.flink.lfxu.function.DelayFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {
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
            + " 'rows-per-second'='10000',\n"
            + " 'fields.f_sequence.kind'='sequence',\n"
            + " 'fields.f_sequence.start'='1',\n"
            + " 'fields.f_sequence.end'='100000',\n"
            + " 'fields.sec_code.min'='100000',\n"
            + " 'fields.sec_code.max'='100003'," +
                "'fields.test_number.kind'='random',\n"
            + " 'fields.f_random_str.length'='4',\n"
            + " 'fields.price.min'='1',\n"
            + " 'fields.price.max'='10'\n"
            + ")");
    tableEnv.executeSql(
            "CREATE VIEW temp_view as select  \n"
                    + " f_sequence,\n"
                    + " sec_code," +
                    "test_number,\n"
                    + " f_random_str,\n"
                    + " price,\n"
                    + " ts " +
                    " from datagen\n");
    tableEnv.executeSql(
        "CREATE TABLE print_table (\n"
            + " f_sequence INT,\n"
            + " sec_code INT," +
                "test_number DECIMAL(6,3),\n"
            + " f_random_str STRING,\n"
            + " price INT,\n"
            + " ts TIMESTAMP(3),"
            + "latency_cout BIGINT\n"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = 'test-sink',\n"
            + "  'properties.bootstrap.servers' = 'localhost:9092',\n"
            + "  'properties.group.id' = 'testGroup',\n"
            + "  'scan.startup.mode' = 'earliest-offset',\n"
            + "  'format' = 'csv'"
            + ")");
    tableEnv.executeSql("insert into print_table select " +
            "f_sequence," +
            "sec_code," +
            "test_number," +
            "f_random_str," +
            "price,ts," +
            "latency(ts) " +
            "from temp_view");
  }

}
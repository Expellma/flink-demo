package com.flink.lfxu;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SQLDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8081);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        tableEnv.executeSql("CREATE TABLE source_table (" +
                "key STRING, " +
                "count_val INT," +
                "eventTime BIGINT," +
                "rt as CAST(eventTime AS TIMESTAMP(3))," +
                "WATERMARK FOR rt as rt - interval '1' second )  " +
                "WITH ('connector' = 'filesystem', 'path' = '/Users/xlf/code/flink-demo/target/classes/data.txt', " +
                "'format' = 'csv', " +
                "'csv.field-delimiter' = '|')");

    tableEnv.executeSql(
        "CREATE TABLE print_table (" +
             "  key STRING ," +
                "end_timestamp TIMESTAMP," +
                "sum_value INT) WITH (\n"
            + " 'connector' = 'print'\n"
            + ")");

    tableEnv.executeSql(
        "CREATE VIEW table_view AS\n"
            + "SELECT\n"
            + "  key,\n"
            + "  TUMBLE_END(rt, INTERVAL '60' SECOND) AS window_end,\n"
            + "  SUM(count_val) AS sum_value\n"
            + "FROM source_table\n"
            + "GROUP BY key, TUMBLE(rt, INTERVAL '60' SECOND)");

        tableEnv.executeSql("insert into print_table " +
                "select * from table_view");

        tableEnv.execute("test");
    }
}

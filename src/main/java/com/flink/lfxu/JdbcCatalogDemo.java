package com.flink.lfxu;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class JdbcCatalogDemo {
    public static void main(String[] args) {
        // 创建表环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 配置JdbcCatalog
        String catalogName = "my_pg_catalog";
        String defaultDatabase = "flink_catalog";
        String username = "postgres";
        String password = "postgres";
        String baseUrl = "jdbc:postgresql://localhost:5432/";

        // 创建JdbcCatalog
        JdbcCatalog pgCatalog = new JdbcCatalog(catalogName, defaultDatabase, username, password, baseUrl);
        tEnv.registerCatalog(catalogName, pgCatalog);

        // 使用Catalog
        tEnv.useCatalog(catalogName);

        // 查询已存在的表
        tEnv.sqlQuery("SELECT * FROM kafka_sink").execute().print();
    }
}

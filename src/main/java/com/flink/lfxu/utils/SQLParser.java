package com.flink.lfxu.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLParser {
    private static Logger log = LoggerFactory.getLogger(SQLParser.class);
    public static void main(String[] args) {
        String filePath = "DemoUDTF.sql"; // 您的 SQL 文件路径
        List<String> queries = parseSQLFile(filePath);

        // 输出解析出的查询语句
        for (String query : queries) {
            System.out.println(query);
        }
    }

    public static List<String> getSqlFromFile(String filePath) {
        List<String> queries = parseSQLFile(filePath);

        // 输出解析出的查询语句
        for (String query : queries) {
            log.info(query);
        }
        return queries;
    }

    private static List<String> parseSQLFile(String filePath) {
        List<String> queries = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        String line;
    try (InputStream inputStream = SQLParser.class.getClassLoader().getResourceAsStream(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            while ((line = br.readLine()) != null) {
                // 如果该行为空或者为注释行，则跳过
                if (line.trim().isEmpty() || line.trim().startsWith("--")) {
                    continue;
                }
                sb.append(line).append("\n"); // 保留原始换行符
                // 如果该行以分号结尾，则认为是一个完整的 SQL 查询语句
                if (line.trim().endsWith(";")) {
                    // 去掉分号
                    sb.deleteCharAt(sb.lastIndexOf(";"));
                    queries.add(sb.toString());
                    sb.setLength(0); // 清空 StringBuilder
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queries;
    }
}

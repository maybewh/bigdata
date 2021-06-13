package com.atguigu.flink.apitest.tableapi;

import com.atguigu.flink.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import java.net.URL;

public class TableTest1_Example {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.读取数据
        URL path = TableTest1_Example.class.getResource("/");
        String url = path.getPath() + "sensor.txt";
        DataStream<String> inputStream = env.readTextFile(url);

        //2. 转换成pojo
        DataStream<SensorReading> dataStream = inputStream.map(line ->{
           String[] fields = line.split(",");
           return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 3.创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 4.创建表环境
        Table datatable = tableEnv.fromDataStream(dataStream);

        // 5. 调用Table API进行转换
        Table resultTable = datatable.select("id, temperature").where("id = 'sensor_1'");

        // 6. 执行SQL
        tableEnv.createTemporaryView("sensor", datatable);
        String sql = "select id, temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();

    }
}

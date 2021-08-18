package com.atguigu.hotitems.analysis;

import com.atguigu.hotitems.analysis.pojo.UserBehavior;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class HotItemsWithSQL {

    public static void main(String[] args) {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String path = HotItemsWithSQL.class.getResource("/").getPath() + File.separator + "/UserBehavior.csv";
        // 2. 从csv文件中读取数据
        DataStream<String> inputStream = env.readTextFile(path);

        // 3. 转换成POJO类，并分配时间戳和watermark
        DataStream<UserBehavior> userBehaviorDataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]),
                    new Integer(fields[2]), new String(fields[3]), new Long(fields[4]));
        }).assignTimestampsAndWatermarks(
                new AssignerWithPeriodicWatermarksAdapter.Strategy<>(
                        new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.of(200, TimeUnit.MILLISECONDS)) {
            @Override
            public long extractTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        }
        ));

        // 4.创建表的执行环境，使用blink版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 5. 将流转换成表
        Table dataTable = tableEnv.fromDataStream(userBehaviorDataStream,
                $("itemId"), $("behavior"),
                $("timestamp").as("ts"));

        // 6.分组开窗
        // Table API
        Table windowAggTable = dataTable.filter($("behavior").isEqual("pv"))
                .window(Slide.over(lit(1).hours()).every(lit(5).minutes()).on($("ts")).as("w"))
                .groupBy($("itemId"), $("w"))
                .select($("itemId"), $("w").end().as("windowEnd"), $("itemId").count().as("cnt"));

    }
}

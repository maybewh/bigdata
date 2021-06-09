package com.atguigu.flink.apitest.transform;

import com.atguigu.flink.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest1Base {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 执行环境并行度设置为1
        env.setParallelism(1);

        DataStream<String> dataStream = env.readTextFile("D:\\development\\code\\idea\\flinkLearn\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> sensorStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 先分组再聚合
        // 分组
        KeyedStream<SensorReading, String> keyedStream = sensorStream.keyBy(SensorReading::getId);

        // 滚动聚合，max和maxBy区别在于，maxBy除了用于max比较的字段以外，其他字段也会更新成最新的，而max只有比较的字段更新，其他字段不变
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");
        DataStream<SensorReading> max = keyedStream.max("temperature");

        resultStream.print("maxBy");
        max.print("max");

        env.execute();

    }
}

package com.atguigu.flink.apitest.transform;

import com.atguigu.flink.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

/**
 * 数据重分区
 */
public class TransformTest6_Partition {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        TransformTest4_MultipleStreams streams = new TransformTest4_MultipleStreams();
        URL path = streams.getClass().getResource("/");

        //从文件中读取数据
        DataStream<String> inputStream = env.readTextFile(path.getPath() + "sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print("input");

        //1.shuffle
        DataStream<String> shuffleStream = inputStream.shuffle();
        shuffleStream.print("shuffle");

        //2. keyBy（Hash，然后取模）
        dataStream.keyBy(SensorReading::getId).print("kyeBy");

        //3. global
        dataStream.global().print("global");

        env.execute();
    }

}

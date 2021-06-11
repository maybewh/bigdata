package com.atguigu.flink.apitest.process.function;

import com.atguigu.flink.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest3_SideOutputCase {

    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从Socket读取数据
        DataStream<String> inputStream = env.socketTextStream("192.168.242.132", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义一个OutputTag,用来表示侧输出流低温流
        OutputTag<SensorReading> lowTempTag = new OutputTag<>("lowTemp");

        // 测试ProcessFunction，自定义侧输出流实现分流操作
        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {


            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                // 判断温度，大于30度，高温流输出到主流，小于低温流输出到侧输出流
                if (sensorReading.getTemperature() > 30) {
                    collector.collect(sensorReading);
                } else {
                    context.output(lowTempTag, sensorReading);
                }

            }
        });

        highTempStream.print("hig-temp");
        highTempStream.getSideOutput(lowTempTag).print("low-temp");

        env.execute();
    }


}

package apitest.window;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class WindowTest3_EventTimeWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1); // 之所以所有数据在截止之后才输出是因为默认的并行度并不为1
        // Flink1.12.X 已经默认就是使用EventTime了，所以不需要这行代码
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("192.168.242.132", 7777);

        // 转换成SensorReading类型，分配时间时间戳
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(
                Duration.ofSeconds(2)).withTimestampAssigner(new MyTimeAssigner()));

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
//        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }

    public static class MyTimeAssigner implements SerializableTimestampAssigner<SensorReading> {

        @Override
        public long extractTimestamp(SensorReading element, long recordTimestamp) {
            return element.getTimestamp() * 1000L;
        }
    }
}

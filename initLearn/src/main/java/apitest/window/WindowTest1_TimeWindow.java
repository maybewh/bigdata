package apitest.window;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowTest1_TimeWindow {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度设为1
        env.setParallelism(1);

        // 从Socket文本中获取数据
        DataStreamSource<String> inputStream = env.socketTextStream("192.168.10.132", 7777);

        // map操作，转换为SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开窗测试

        // 1. 增量聚合函数（这里简单统计每个key组里传感器信息的总数）
        DataStream<Integer> resultStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() { // 新建累加器
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) { // 每个数据在上次的基础上累加
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    // 分区合并结果（TimeWindow一般用不到，SessionWindow可能需要考虑合并）
                    // todo 问题：为什么只有会话窗口需要merge操作？？？ 因为时间长度内，可能会有多个窗口，窗口在时间上并不连续？？
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        resultStream.print("result");

        env.execute();

    }
}

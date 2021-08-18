package apitest.process.function;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTest2_ApplicationCase {

    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);
        // 从socket中获取数据
        DataStream<String> inputStream = env.socketTextStream("192.168.10.132", 7777);
        // 转换数据为SensorReading类型
        DataStream<SensorReading> sensorReadingStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 如果存在连续10s内温度持续上升的情况，则报警
        sensorReadingStream.keyBy(SensorReading::getId)
                .process(new TempConsIncreWarning(Time.seconds(10).toMilliseconds()))
                .print();

        env.execute();

    }

    public static class TempConsIncreWarning extends KeyedProcessFunction<String, SensorReading, String> {

        private Long interval; // 报警的时间间隔

        private ValueState<Double> lastTemperature; // 上一个温度值

        // 最近一次定时器的触发时间（报警时间）
        private ValueState<Long> recentTimerTimeStamp;

        public TempConsIncreWarning(Long interval) {
            this.interval = interval;
        }

        /**
         * 如果存在连续10s内温度持续上升的情况，则报警
         * @param sensorReading
         * @param context
         * @param collector
         * @throws Exception
         */
        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<String> collector) throws Exception {

        }
    }
}

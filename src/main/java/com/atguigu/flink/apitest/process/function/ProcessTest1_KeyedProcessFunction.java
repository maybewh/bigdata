package com.atguigu.flink.apitest.process.function;

import com.atguigu.flink.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessTest1_KeyedProcessFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Socket文本流
        DataStream<String> inputStream = env.socketTextStream("192.168.242.132",7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 测试KeyedProcessFunction，先分组然后自定义处理
        dataStream.keyBy("id")
            .process( new MyProcess() )
            .print();

        env.execute();
    }

    public static class MyProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {

        ValueState<Long> tsTimeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading sensorReading, Context context, Collector<Integer> collector) throws Exception {
            collector.collect(sensorReading.getId().length());

            // context
            // Timestamp of the element currently being processed or timestamp of a firing timer.
            context.timestamp();

            // get the key of the element currently being processed or timestamp of a firing timer.
            context.getCurrentKey();

//            context.output(OutputTag);
            context.timerService().currentProcessingTime();
            context.timerService().currentWatermark();

            // 在处理时间的5秒延迟后触发
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 5000L);
            tsTimeState.update(context.timerService().currentProcessingTime() + 1000L);

            context.timerService().registerEventTimeTimer((sensorReading.getTimestamp() + 10) * 1000L);
            // 删除指定指定时间触发的定时器
            context.timerService().deleteProcessingTimeTimer(tsTimeState.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(timestamp + " 定时器触发");
            ctx.getCurrentKey();
//            ctx.output();
            ctx.timeDomain();
        }

        @Override
        public void close() throws Exception {
            tsTimeState.clear();
        }
    }
}

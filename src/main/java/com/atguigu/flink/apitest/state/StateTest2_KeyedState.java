package com.atguigu.flink.apitest.state;

import com.atguigu.flink.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest2_KeyedState {

    public static void main(String[] args) {

        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度 = 1
        env.setParallelism(1);
        // 从本地socket读取数据
        DataStream<String> inputStream = env.socketTextStream("192.168.10.132", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 使用自定义map方法，里面使用我们自定义的keyed state
        DataStream<Integer> resultStream = dataStream.keyBy(SensorReading::getId)
                .map(new MyMapper());
    }

    public static class MyMapper extends RichMapFunction<SensorReading, Integer> {

        private ValueState<Integer> valueState;

        // 其他类型状态声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {

            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("my-int", Integer.class));

            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));

            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));

            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reduce", new ReduceFunction<SensorReading>() {
                @Override
                public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                    return null;
                }
            }, SensorReading.class));
        }

        /**
         * 这里就简单的统计每个 传感器的 信息数量
         * @param value
         * @return
         * @throws Exception
         */
        @Override
        public Integer map(SensorReading value) throws Exception {
            // 其他状态API调用
            // list state
            for (String str:
                 myListState.get()) {
                System.out.println(str);
            }
            myListState.add("hello");

            // map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.remove("2");

            // reducing state
            myReducingState.add(value);
            myMapState.clear();

            Integer count = valueState.value();
            count = count == null ? 0 :count;
            ++count;
            valueState.update(count);
            return count;
        }

        @Override
        public void close() throws Exception {

        }
    }
}

package com.atguigu.flink.apitest.transform;


import com.atguigu.flink.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

/**
 *  将split和select用sideOutput来代替,sideOutput可以进行连续的分流，在1.12版本中已经将split和select对应的API已被删除
 */
public class TransformTest4_MultipleStreams {

    // 使用side_output来进行分流，并且可以进行连续的分流
    // 1.定义标签
    private static final OutputTag<SensorReading> highTemperature = new OutputTag<SensorReading>("high"){};
    private static final OutputTag<SensorReading> lowTemperature = new OutputTag<SensorReading>("low"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        TransformTest4_MultipleStreams streams = new TransformTest4_MultipleStreams();
        URL path = streams.getClass().getResource("/");

        //从文件中读取数据
        DataStream<String> inputStream = env.readTextFile(path.getPath() + "sensor.txt");

        // 转换成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
           String[] fields = line.split(",");
            SensorReading sensorReading = new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            return sensorReading;
        });

        // dataStream为总的数据流
        SingleOutputStreamOperator<SensorReading> outputStream = dataStream.process(
                new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {

                        if (sensorReading.getTemperature() == null) {
                            collector.collect(sensorReading);
                        } else if (sensorReading.getTemperature() > 30) {
                            context.output(highTemperature, sensorReading);
                        } else {
                            context.output(lowTemperature, sensorReading);
                        }
                    }
                }
        );

        // 高温
        outputStream.getSideOutput(highTemperature).print("high");
        // 非高温
        outputStream.getSideOutput(lowTemperature).print("low");
        // 其他
        outputStream.print("other");


        //2.合并连接，将高温流转换为二元组类型，与低温流连接合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = outputStream.getSideOutput(highTemperature).map(
                new MapFunction<SensorReading, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(SensorReading value) throws Exception {
                        return new Tuple2<>(value.getId(), value.getTemperature());
                    }
                }
        );

        // 连接
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(
                outputStream.getSideOutput(lowTemperature));

        // 由于合并时，两条流的数据结构不一样，所以需要做类型转换
        DataStream<Object> resultStream = connectedStreams.map(
                new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
                    @Override
                    public Object map1(Tuple2<String, Double> value) throws Exception {
                        return new Tuple3<>(value.f0, value.f1, "high temp warning");
                    }

                    @Override
                    public Object map2(SensorReading value) throws Exception {
                        return new Tuple2<>(value.getId(), "normal");
                    }
                }
        );

        resultStream.print("connect");

        // Union 可以合并多条流，Union的数据结构必须是一致的

        env.execute();

    }
}

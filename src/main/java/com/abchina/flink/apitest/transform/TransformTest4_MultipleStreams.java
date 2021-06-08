package com.abchina.flink.apitest.transform;


import com.abchina.flink.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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

        //从文件中读取数据
        DataStream<String> inputStream = env.readTextFile("E:\\VMware\\tmpWanghui\\flinkLearn\\src\\main\\resources\\sensor.txt");

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

        env.execute();

    }
}

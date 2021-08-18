package apitest.transform;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

public class TransformTest_5RichFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        TransformTest4_MultipleStreams streams = new TransformTest4_MultipleStreams();
        URL path = streams.getClass().getResource("/");

        //从文件中读取数据
        DataStream<String> inputStream = env.readTextFile(path.getPath() + "sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(
                line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                }
        );

        //
        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();
        env.execute();
    }

    // 传统的Function不能获取上下文信息，只能处理当前的数据，不能和其他数据交互
    public static class MyMapper0 implements MapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    // 实现自定义富函数类（RichFunction是一个抽象类）
    public static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open"); //如连接hdfs
        }

        @Override
        public void close() throws Exception {
            System.out.println("close"); // 关闭与hdfs的连接或者hbase
        }
    }


}

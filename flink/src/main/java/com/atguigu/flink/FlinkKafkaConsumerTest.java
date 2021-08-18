package com.atguigu.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.List;
import java.util.Properties;

public class FlinkKafkaConsumerTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度设置为1
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.242.130:9092,192.168.242.131:9092,192.168.242.132:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");

        // 从Kafka中读取数据
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));
//        inputStream.print();


        SingleOutputStreamOperator<List<String>> counts = inputStream.timeWindowAll(Time.seconds(20))
                .trigger(new CountTrigger(19)).apply(
                        new ListFunction<String>()
                );
        counts.print("outs");
        env.execute();
    }
}

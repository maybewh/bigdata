package apitest.transform;

import apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SinkTest1_Kafka {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度设为1
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // todo 修改
        properties.setProperty("group.id", "consumer-group");// todo 修改
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 从Kafka中读取数据 todo SimpleStringSchema
        DataStream<String> inputStream = env.addSource(
                new FlinkKafkaConsumer<String>("sensor",new SimpleStringSchema(), properties));

        // 序列化从Kafka中读取数据
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        // 将数据写入Kafka
        dataStream.addSink(
                new FlinkKafkaProducer<String>(
                        "192.168.10.30:9092", "sinktest",
                        new SimpleStringSchema()));

        env.execute();

    }
}

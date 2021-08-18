package com.atguigu.hotitems.analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

public class HotItemsKafkaProducerUtil {

    public static void main(String[] args) throws Exception {
        writeToKafka("hotitems");
    }

    public static void writeToKafka(String topic) throws Exception {

        // kafka配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.242.130:9092,192.168.242.131:9092,192.168.242.132:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定义一个Kafka的Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        // 用缓冲的方式读取文件
        String path = HotItemsKafkaProducerUtil.class.getResource("/UserBehavior.csv").getPath();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
            // 发送数据
            producer.send(record);
        }
        producer.close();
    }
}

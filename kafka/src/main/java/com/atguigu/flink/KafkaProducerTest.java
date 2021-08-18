package com.atguigu.flink;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerTest {


    private static final String brokerList="192.168.242.130:9092,192.168.242.131:9092,192.168.242.132:9092";

    private static final String topic="test";

    public static void main(String[] args) {

        Properties props = new Properties();
        //设置key序列化器
        props.put("key.serializer", StringSerializer.class.getName());
        //设置value序列化器
        props.put("value.serializer", StringSerializer.class.getName());
        //设置集群地址
        props.put("bootstrap.servers", brokerList);
        props.put("acks", "all");
        //设置重试次数
        props.put("retries", 2);
        props.put("batch.size", 10);
        props.put("request.timeout.ms", 190000);
        props.put("auto.create.topics.enable", "true");

        KafkaProducer<String, String> producer=new KafkaProducer<String, String>(props);


        try {
      /*      Future<RecordMetadata> send = producer.send(record);
            RecordMetadata recordMetadata = send.get();
            System.out.println("topic:"+recordMetadata.topic());
            System.out.println("分区:"+recordMetadata.partition());
            System.out.println("offset:"+recordMetadata.offset());*/
            for (int i = 0; i < 101; i++) {

                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "kafka-demo_"+i , "hello kafka_" + i);
                Thread.sleep(1000);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e==null){
                            System.out.println("topic:"+recordMetadata.topic());
                            System.out.println("分区:"+recordMetadata.partition());
                            System.out.println("offset:"+recordMetadata.offset());
                        } else {
                            System.out.println("exception:" + e);
                        }
                    }
                });
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }
}

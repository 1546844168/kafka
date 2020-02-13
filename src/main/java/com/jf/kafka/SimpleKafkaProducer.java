package com.jf.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author 江峰
 * @create 2020-02-12   18:15
 */
public class SimpleKafkaProducer {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();

        //broker地址
        props.put("bootstrap.servers", "139.224.103.236:9092");

        //请求时候需要验证
        props.put("acks", "all");

        //请求失败时候需要重试
        props.put("retries", 0);

        //内存缓存区大小
        props.put("buffer.memory", 33554432);

        //指定消息key序列化方式
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        //指定消息本身的序列化方式
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10000; i++) {
            producer.send(new ProducerRecord<String, String>("five", "value: " + i));
            System.out.println("Message sent successfully");
            TimeUnit.MILLISECONDS.sleep(500);
        }
        producer.close();
    }
}
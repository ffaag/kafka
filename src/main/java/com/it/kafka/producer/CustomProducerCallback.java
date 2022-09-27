package com.it.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author ZuYingFang
 * @time 2022-02-28 16:38
 * @description 带回调函数的异步发送
 */
public class CustomProducerCallback {

    public static void main(String[] args) throws InterruptedException {

        // 0 创建一个配置
        Properties properties = new Properties();

        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");

        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 1 创建kafka生产者对象，如我们之前传输的hello，key为空
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 2 发送数据
        for (int i = 0; i < 1000; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "hello" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        System.out.println("主题：" + recordMetadata.topic() + "分区：" + recordMetadata.partition());
                    }

                }
            });
            Thread.sleep(10);
        }

        // 3 关闭资源
        kafkaProducer.close();

    }

}

package com.it.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author ZuYingFang
 * @time 2022-02-28 16:38
 * @description 默认分区器DefaultPartitioner的规则以及自定义分区
 */
public class CustomProducerCallbackPartition {

    public static void main(String[] args) {

        // 0 创建一个配置
        Properties properties = new Properties();

        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");

        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 关联自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.it.kafka.producer.MyPartitions");

        // 1 创建kafka生产者对象，如我们之前传输的hello，key为空
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 2 发送数据
        for (int i = 0; i < 5; i++) {
            // kafkaProducer.send(new ProducerRecord<>("first", 2, "a", "hell" + i), new Callback() { // 指定分区
            // kafkaProducer.send(new ProducerRecord<>("first", "a", "hell" + i), new Callback() {  // 指定key
            kafkaProducer.send(new ProducerRecord<>("first", "hello" + i), new Callback() {  // 黏性规则
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        System.out.println("主题：" + recordMetadata.topic() + "分区：" + recordMetadata.partition());
                    }

                }
            });
        }

        // 3 关闭资源
        kafkaProducer.close();

    }

}

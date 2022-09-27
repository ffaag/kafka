package com.it.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author ZuYingFang
 * @time 2022-02-28 16:38
 * @description 数据可靠性，与ack有关，0一般不用，1一般，最可靠为ACK级别设置为-1 + 分区副本大于等于2 + ISR里应答的最小副本数量大于等于2
 */
public class CustomProducerAck {

    public static void main(String[] args) {

        // 0 创建一个配置
        Properties properties = new Properties();

        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");

        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 配置对应的acks
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        // 重新次数，即sender重试，默认为Int最大值
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");


        // 1 创建kafka生产者对象，如我们之前传输的hello，key为空
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        // 2 发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "hello" + i));
        }

        // 3 关闭资源
        kafkaProducer.close();

    }

}

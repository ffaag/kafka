package com.it.kafka.producer;

import com.sun.xml.internal.txw2.output.StreamSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.beans.PropertyDescriptor;
import java.net.ProxySelector;
import java.util.Properties;

/**
 * @author ZuYingFang
 * @time 2022-02-28 20:32
 * @description 提高生产者吞吐量
 */
public class CustomProducerParameters {

    public static void main(String[] args) {

        // 0 配置信息
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 提高吞吐量
        // 1 缓冲区大小
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);   // 32M

        // 2 批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // 3 linger.ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        // 4 压缩
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        // 1 创建生产者
        KafkaProducer<Object, Object> kafkaProducer = new KafkaProducer<>(properties);

        // 2 发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "hello" + i));
        }

        // 3 关闭资源
        kafkaProducer.close();

    }


}

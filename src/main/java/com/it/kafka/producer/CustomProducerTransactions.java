package com.it.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author ZuYingFang
 * @time 2022-02-28 16:38
 * @description 数据去重之生产者事务
 */
public class CustomProducerTransactions {

    public static void main(String[] args) {

        // 0 创建一个配置
        Properties properties = new Properties();

        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");

        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 指定事务id，必须开发人员手动指定，全局唯一
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tranaction_id_01");

        // 1 创建kafka生产者对象，如我们之前传输的hello，key为空
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 1 初始化事务
        kafkaProducer.initTransactions();

        // 2 开启事务
        kafkaProducer.beginTransaction();

        try {
            // 2 发送数据
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>("first", "hello" + i));
            }

            // 模拟失败
            int i = 1 / 0;

            // 3 提交事务
            kafkaProducer.commitTransaction();
        } catch (ProducerFencedException e) {
            // 4 放弃事务（类似于回滚事务的操作）
            kafkaProducer.abortTransaction();
        } finally {
            // 3 关闭资源
            kafkaProducer.close();
        }


    }

}

package com.it.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author ZuYingFang
 * @time 2022-11-20 15:31
 * @description
 */
public class MyPartitioner implements Partitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取消息
        String msgValue = value.toString();

        // 创建 partition
        int partition;

        // 判断消息是否包含 atguigu
        if (msgValue.contains("atguigu")){
            partition = 0;
        }else {
            partition = 1;
        }

        // 返回分区号
        return partition;

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

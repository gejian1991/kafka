package com.cn.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区管理器
 */
public class MyPartitioner implements Partitioner {
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        System.out.println("使用自己的分区管理器");
        if(bytes==null){
            return 0;
        }else {
            return 2;
        }

    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}

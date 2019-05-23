package com.cn.Admin;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.JavaConversions;

import java.util.List;
import java.util.Properties;

public class KafkaAdmin {
    //创建topic
    public static void createTopic(){
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181/kafka", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        System.out.println(JaasUtils.isZkSecurityEnabled());
        //AdminUtils.addPartitions();
        AdminUtils.createTopic(zkUtils, "t1", 1, 1, new Properties(), AdminUtils.createTopic$default$6());
        zkUtils.close();
    }

    //删除topic
    public static void deleteTopic(){
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181/kafka", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        AdminUtils.deleteTopic(zkUtils, "t1");
        zkUtils.close();
    }

    //列出所有topic
    public static void listTopic(){
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181/kafka", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        List<String> list = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
    for (String s : list) {
        System.out.println(s);
    }
    zkUtils.close(); }
}

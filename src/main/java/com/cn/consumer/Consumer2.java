package com.cn.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class Consumer2 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //指定kafka服务器地址 如果是集群可以指定多个  但是就算只指定一个他也会去集群环境下寻找其他的 节点地 址
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        //key反序列化器
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        //value反序列化器
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        //取消自动提交
        properties.setProperty("enable.auto.commit", "false");
        //定义消费者群组
        properties.setProperty("group.id","1111");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("my-topic"));


        //kafka区别于其他MQ，消费消息需要去kafka服务器去拉取消息
        try{
            while (true){
                //间隔多久，拉取一次消息
                ConsumerRecords<String, String> poll = consumer.poll(500);
                for (ConsumerRecord<String, String> context : poll) {
                    System.out.println("消息所在分区"+context.partition()+"---消息偏移量"+context.offset()+"--key:"+context.key()+
                            "----value:"+context.value());
                    System.out.println("处理消费到的消息组2");
                }
                //异步提交偏移量
                //consumer.commitAsync();
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            try{
                //同步提交偏移量，同步提交失败kafka会自动重试
                consumer.commitSync();
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                consumer.close();
            }
        }
    }
}

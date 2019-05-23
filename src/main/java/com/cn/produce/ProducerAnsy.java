package com.cn.produce;

import com.cn.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * kafka异步回调确认
 */
public class ProducerAnsy {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //指定kafka服务器地址 如果是集群可以指定多个  但是就算只指定一个他也会去集群环境下寻找其他的 节点地 址
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        //key序列化器,用来确认消息放入那个分区的
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        //value序列化器
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //自定义分区管理器
        properties.setProperty("partitioner.class", MyPartitioner.class.getName());
        //压缩,有很多种
        properties.setProperty("compression.type", "lz4");


        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String> (properties);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-topic",1,"test.Key","hello");
        //异步确认方法
        kafkaProducer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if(exception!=null){
                    exception.printStackTrace();
                }
                if(null!=metadata){
                    System.out.println(metadata.topic());
                }
            }
        });
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}

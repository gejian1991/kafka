package com.cn.produce;

import com.cn.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {
    public static void main(String[] args)  throws Exception{
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
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-topic",null,"hello");
        kafkaProducer.send(record);
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("my-topic",null,"hello1");
        kafkaProducer.send(record2);
        //同步确认方法
       /*  Future<RecordMetadata> send = kafkaProducer.send(record);
        RecordMetadata recordMetadata = send.get();//阻塞在这里
        System.out.println(recordMetadata.offset());*/

        //异步确认方法
        //见ProducerAnsy

        //不需要确认
       /* kafkaProducer.send(record);*/

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

package com.kdl.consumer;

import com.github.io.protocol.utils.HexStringUtil;
import com.kdl.calc.PerformanceCalc;
import common.Constant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * kafka消费者
 *
 * @author Kong, created on 2018-08-14T14:31.
 * @since 1.0-SNAPSHOT
 */
@Component
public class KafkaConsumer {

    private Logger logger = LoggerFactory.getLogger(KafkaConsumer.class) ;

    private long time ;

    /**
     * 数据条数
     */
    @Value("${kafkaMQ.data_number}")
    Integer dataNumber ;

    // 基础消费
    @KafkaListener(id = "id0",topics = Constant.Topic.KONG)
    // 分区消费
//    @KafkaListener(id = "id0",topicPartitions = { @TopicPartition(topic = Constant.Topic.KONG, partitions = { "0","1" }) })
    // 消费多个Topic
//    @KafkaListeners(value = {@KafkaListener(topics = Constant.Topic.KONG),@KafkaListener(topics = Constant.Topic.TEST)})
    public void consumer(ConsumerRecord<String, byte[]> record){

        logger.info("now consumer the message the topic is: {}, it's offset is :{} and the value is :{}" ,record.topic(),record.offset(),HexStringUtil.toHexString(record.value()));
        logger.info("partition:{}",record.partition());
        // 记录消费数据条数
        if (PerformanceCalc.consumeMsgNumber.addAndGet(1) == 1){
            time = System.currentTimeMillis() ;
        }
        if (PerformanceCalc.consumeMsgNumber.get() == dataNumber){
            logger.info("consumer num {} , times {}" , dataNumber ,  (System.currentTimeMillis()- time)) ;
        }
        System.out.println(HexStringUtil.toHexString(record.value()));
        // 记录消费数据条数
        if (PerformanceCalc.consumeMsgNumber.addAndGet(1) == 1){
            time = System.currentTimeMillis() ;
        }

        if (PerformanceCalc.consumeMsgNumber.get() == dataNumber){
            logger.info("consumer num {} , times {}" , dataNumber ,  (System.currentTimeMillis()- time)) ;
        }
    }
}

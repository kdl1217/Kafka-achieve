package com.kdl.producer;

import com.kdl.calc.PerformanceCalc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;

/**
 * Kafka生成者
 *
 * @author Kong, created on 2018-08-14T14:25.
 * @since 1.2.0-SNAPSHOT
 */
@Component
public class KafkaProducer {

    private Logger logger = LoggerFactory.getLogger(KafkaProducer.class) ;

    @Autowired
    private KafkaTemplate<String,byte[]> template ;

    /**
     * 数据条数
     */
    @Value("${kafkaMQ.data_number}")
    Integer dataNumber ;

    /**
     * 发送数据
     * @param topic
     * @param msg
     */
    public void sendMsg(String topic,byte[] msg,long time){
        // 发送数据
        ListenableFuture<SendResult<String, byte[]>> listenableFuture = template.send(topic,msg);
        //发送成功回调
        SuccessCallback<SendResult<String, byte[]>> successCallback = result -> {
            // 成功业务逻辑
            if (PerformanceCalc.produceMsgNumber.addAndGet(1) == dataNumber) {
                logger.info("producer num {} , times {}" , dataNumber ,  (System.currentTimeMillis()- time)) ;
            }
        };
        //发送失败回调
        FailureCallback failureCallback = ex -> {
            // 失败业务逻辑
            logger.info("ERROR!!!") ;
        };
        listenableFuture.addCallback(successCallback, failureCallback);
    }

}

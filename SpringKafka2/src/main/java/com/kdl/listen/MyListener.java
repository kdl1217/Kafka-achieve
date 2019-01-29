package com.kdl.listen;

import com.github.io.protocol.utils.HexStringUtil;
import common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

/**
 * Kafka监听器
 *
 * @author Kong, created on 2018-08-14T14:14.
 * @since 1.0-SNAPSHOT
 */
@Component
public class MyListener {

    private static final Logger log= LoggerFactory.getLogger(MyListener.class);

    @Autowired
    private KafkaTemplate<String,byte[]> template ;

    //声明consumerID为id0，监听topicName为kong的Topic
    @KafkaListener(id = "id0",topics = Constant.Topic.KONG, errorHandler = "consumerAwareErrorHandler")
//    @SendTo
    public void listen(byte[] msgData) {
        log.info("receive : "+ HexStringUtil.toHexString(msgData));
        throw new RuntimeException("fail") ;
    }

    /**
     * 异常处理
     * @return
     */
    @Bean
    public ConsumerAwareListenerErrorHandler consumerAwareErrorHandler() {
        return (message, e, consumer) -> {
            log.info("consumerAwareErrorHandler receive : "+ HexStringUtil.toHexString((byte[]) message.getPayload()));
            template.send(Constant.Topic.KONG,(byte[]) message.getPayload()) ;
            return null;
        };
    }
}

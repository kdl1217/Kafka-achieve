package com.kdl.listen;

import com.github.io.protocol.utils.HexStringUtil;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

/**
 * Kafka 发送消息监听器
 *
 * @author Kong, created on 2019-01-29T14:42.
 * @since 1.0-SNAPSHOT
 */
@Component
public class KafkaSendResultListener implements ProducerListener<String,byte[]> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSendResultListener.class);


    @Override
    public void onSuccess(String topic, Integer partition, String key, byte[] value, RecordMetadata recordMetadata) {
        log.info("Message send success : " + HexStringUtil.toHexString(value));
    }

    @Override
    public void onError(String topic, Integer partition, String key, byte[] value, Exception exception) {
        log.info("Message send error : " + HexStringUtil.toHexString(value));
    }

}

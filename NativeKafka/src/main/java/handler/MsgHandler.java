package handler;

import org.apache.tomcat.util.buf.HexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消息订阅
 *
 * @author Kong, created on 2019-01-27T11:36.
 * @since 1.0-SNAPSHOT
 */
public class MsgHandler implements IMessageHandler {

    /**
     * 日志
     */
    private Logger logger = LoggerFactory.getLogger(MsgHandler.class) ;


    @Override
    public void messageReceive(String topic, int partition, long offset, String key, byte[] value) {
        System.out.println("topic:"+topic+",partition:"+partition+",offset:"+offset);

        logger.info("key: {}, value: {}",key, HexUtils.toHexString(value));
    }
}

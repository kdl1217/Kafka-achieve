package handler;

/**
 * 消息Handler
 *
 * @author Kong, created on 2019-01-27T11:25.
 * @since 1.0-SNAPSHOT
 */
public interface IMessageHandler {

    /**
     * 获取消费信息
     * @param topic   消息类别
     * @param partition 分区
     * @param offset  偏移
     * @param key     Key
     * @param value   Value
     */
    void messageReceive(String topic, int partition, long offset, String key, byte[] value);

}

package consume;

import common.Constant;
import handler.IMessageHandler;
import handler.MsgHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.tomcat.util.buf.HexUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * 消费者模型
 *  自动提交-订阅消息
 * @author Kong, created on 2019-01-26T23:42.
 * @since 1.0-SNAPSHOT
 */
public class KConsumer {

    /**
     * 日志
     */
    private static Logger s_logger = LoggerFactory.getLogger(Consumer.class);

    /**
     * 消费者
     */
    private KafkaConsumer<String, byte[]> consumer;

    /**
     * 订阅消息Handler
     */
    private IMessageHandler messageHandler;

    /**
     * 消息读取
     */
    private KConsumer.MsgReader msgReader;

    /**
     * 订阅主题
     */
    private List<String> topicList;

    /**
     * 构造函数
     *
     * @param topic 订阅主题
     */
    public KConsumer(String topic, IMessageHandler messageHandler) {
        if (StringUtils.isEmpty(topic)) {
            throw new IllegalArgumentException();
        }

        this.messageHandler = messageHandler;
        this.msgReader = new KConsumer.MsgReader();

        Properties props = new Properties();
        //设置消费的Topic组
        this.topicList = Arrays.asList(topic);
        // 该地址是集群的子集，用来探测集群。 bootstrap.servers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.138:9092,192.168.31.139:9092,192.168.31.140:9092");
        // cousumer的分组id group.id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kong-1");
        // 自动提交offsets enable.auto.commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
        // 如果采用latest，消费者只能得道其启动后，生产者生产的消息 auto.offset.reset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Consumer向集群发送自己的心跳，超时则认为Consumer已经死了，kafka会把它的分区分配给其他进程 session.timeout.ms
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // 反序列化器 key.deserializer   value.deserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        //拉取循环中的处理数据量 max.poll.records
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topicList);
    }

    /**
     * 启动消息订阅
     */
    public void start(){
        this.msgReader.start();
    }

    private class MsgReader extends Thread {
        private MsgReader() {
        }

        public void run() {
            while(true) {
                try {
                    ConsumerRecords<String, byte[]> records = KConsumer.this.consumer.poll(100L);
                    Iterator recordIterator = records.iterator();

                    while(recordIterator.hasNext()) {
                        ConsumerRecord record = (ConsumerRecord)recordIterator.next();

                        try {
                            if (KConsumer.this.messageHandler != null) {
                                KConsumer.this.messageHandler.messageReceive(record.topic(), record.partition(), record.offset(), (String)record.key(), (byte[])record.value());
                            }
                        } catch (Exception var5) {
                            var5.printStackTrace();
                        }
                    }
                } catch (Exception var6) {
                    var6.printStackTrace();
                }
            }

        }
    }


    /**********************************测试例子*******start**********************************/
    public static void main(String[] args) {
        MsgHandler msgHandler = new MsgHandler();
        // Topic
        String topic = Constant.Topic.KONG ;
        // 获取消费者
        KConsumer consumer = new KConsumer(topic, msgHandler) ;
        //启动订阅器
        consumer.start();
    }
}

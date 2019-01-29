package com.kdl.calc;

import com.github.io.protocol.utils.HexStringUtil;
import com.kdl.consumer.KafkaConsumer;
import com.kdl.producer.KafkaProducer;
import common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 性能计算
 *
 * @author Kong, created on 2018-08-14T14:37.
 * @since 1.0-SNAPSHOT
 */
@Component
public class PerformanceCalc {

    private Logger logger = LoggerFactory.getLogger(PerformanceCalc.class) ;

    @Autowired
    private KafkaConsumer consumer ;

    @Autowired
    private KafkaProducer producer ;


    /**
     * 数据条数
     */
    @Value("${kafkaMQ.data_number}")
    Integer dataNumber ;

    /**
     * 生产者线程数量
     */
    @Value("${kafkaMQ.thread_number}")
    Integer threadNumber ;


    /**
     *  持续发送数据条数
     */
    public static AtomicInteger produceMsgNumber = new AtomicInteger(0);

    /**
     * 消费数据条数
     */
    public static AtomicInteger consumeMsgNumber = new AtomicInteger(0);

    /**
     * 监控线程情况
     */
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    /**
     * 启动项
     */
    public void start(){
        long start = System.currentTimeMillis() ;
        // 初始化生产者
//        continuousProducer(start) ;
        monitoring() ;
    }


    /**
     * 持续发送数据
     */
    private void continuousProducer(long start){
        while (true) {
            String msg = String.valueOf(System.currentTimeMillis()) ;
            producer.sendMsg(Constant.Topic.KONG, HexStringUtil.parseBytes(msg),start) ;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 数据监控
     */
    private void monitoring(){
        long start = System.currentTimeMillis() ;
        scheduledExecutorService.scheduleAtFixedRate(()->{
            logger.info("producer num {} , consumer num {}, times {}" , produceMsgNumber.get() , consumeMsgNumber.get() , (System.currentTimeMillis()- start) );
        },200,200, TimeUnit.MILLISECONDS) ;
    }

}

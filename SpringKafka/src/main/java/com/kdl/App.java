package com.kdl;

import com.kdl.calc.PerformanceCalc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * SpringBoot
 * SpringKafka Version 1.3.9
 * @author Kong, created on 2018-08-14T14:13.
 * @since 1.2.0-SNAPSHOT
 */
@SpringBootApplication
public class App implements CommandLineRunner {

    private Logger logger = LoggerFactory.getLogger(App.class) ;

    @Autowired
    private PerformanceCalc performanceCalc ;

    public static void main(String[] args) {
        SpringApplication.run(App.class,args) ;
    }

    @Override
    public void run(String... args) throws Exception {
        performanceCalc.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("shutdown ...");
        }));
    }


}

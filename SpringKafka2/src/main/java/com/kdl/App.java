package com.kdl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * SpringBoot
 * SpringKafka Version 2.2.3
 * @author Kong, created on 2018-08-14T14:13.
 * @since 1.2.0-SNAPSHOT
 */
@SpringBootApplication
public class App implements CommandLineRunner {

    private Logger logger = LoggerFactory.getLogger(App.class) ;


    public static void main(String[] args) {
        SpringApplication.run(App.class,args) ;
    }

    @Override
    public void run(String... args) throws Exception {

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("shutdown ...");
        }));
    }


}

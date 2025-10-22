package com.mobile.outalarm2;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@MapperScan("com.mobile.outalarm2.dao")
@EnableScheduling
public class OutAlarm2Application {

    public static void main(String[] args) {
        SpringApplication.run(OutAlarm2Application.class, args);
    }

}

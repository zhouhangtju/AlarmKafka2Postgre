package com.mobile.outalarm2;

import cn.hutool.json.ObjectMapper;
import com.mobile.outalarm2.db.Message;
import com.mobile.outalarm2.service.AlarmResultService;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@SpringBootTest
class DemoApplicationTests {

    @Autowired
    private AlarmResultService resultService;
    @Test
    void test() throws ParseException {
     String s=  "2025-08-25 16:22:15.000";
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date parse = sdf2.parse(s);
        System.out.printf(String.valueOf(parse));
    }
}

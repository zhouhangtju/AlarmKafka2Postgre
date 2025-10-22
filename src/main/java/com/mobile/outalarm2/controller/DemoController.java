package com.mobile.outalarm2.controller;


import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.mobile.outalarm2.common.CsvUtil;
import com.mobile.outalarm2.dao.AlarmResultAlldayDao;
import com.mobile.outalarm2.dao.AlarmResultDao;
import com.mobile.outalarm2.db.AlarmResult;
import com.mobile.outalarm2.db.AlarmResultAllday;
import com.mobile.outalarm2.service.AlarmResultService;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/test")
@Slf4j
@Api(value = "测试接口", tags = {"测试接口"})
public class DemoController {

    @Autowired
    private AlarmResultService alarmResultService;
//    @Autowired
//    private KafkaProducer kafkaProducer;
    @Autowired
    private AlarmResultAlldayDao alarmResultDao;


//    @GetMapping("/send")
//    public void sendMsg(){
//        kafkaProducer.test();
//    }

    @GetMapping("/test")
    public void test(){
        String dd = "2025082516";
        alarmResultService.aggregationAllday(dd);
    }

}





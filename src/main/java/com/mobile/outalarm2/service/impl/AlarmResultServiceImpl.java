package com.mobile.outalarm2.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mobile.outalarm2.dao.AlarmResultAlldayDao;
import com.mobile.outalarm2.dao.AlarmResultDao;
import com.mobile.outalarm2.db.AlarmResult;
import com.mobile.outalarm2.db.AlarmResultAllday;
import com.mobile.outalarm2.db.Business;
import com.mobile.outalarm2.db.NewBusiness;
import com.mobile.outalarm2.service.AlarmResultService;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AlarmResultServiceImpl implements AlarmResultService {

    @Autowired
    private AlarmResultDao alarmResultDao;
    @Autowired
    private AlarmResultAlldayDao alarmResultAlldayDao;
    @Qualifier("asyncServiceExecutor")
    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;
    @Override
    public void aggregationAllday(String dd) {

//       QueryWrapper<AlarmResult> queryWrapper = new QueryWrapper<>();
//        queryWrapper.select("key").eq("ds",dd);
        try{
            //List<AlarmResult> alarmResults = alarmResultDao.selectList(queryWrapper);
           List<AlarmResult> alarmResults = alarmResultDao.selectAllKey("alarm_result_"+dd);
            log.info("总数据{}",alarmResults.size());
            Set<String> set = alarmResults.stream().map(alarmResult -> alarmResult.getKey()).collect(Collectors.toSet());
            System.out.printf(String.valueOf(set.size()));
            List<String> collect = set.stream().collect(Collectors.toList());
            int total = set.size();
            System.out.println("去重后的key" + total);
            int batch = 5000;
            int times = total / batch;
            for (int i = 0; i <= times; i++) {
                if ((i + 1) * batch >= total) {
                    List<String> subList = collect.subList(i * batch, total);
                    threadPoolTaskExecutor.execute(() -> {
                        insertAllDay(subList,dd);
                    });
                     log.info("===========第" + String.valueOf(i) + "批");
                } else {
                    List<String> subList = collect.subList(i * batch, (i + 1) * batch);
                    threadPoolTaskExecutor.execute(() -> {
                        insertAllDay(subList,dd);
                    });
                     log.info("===========第" + String.valueOf(i) + "批");
                }
            }
        }catch (Exception e){
            log.info("聚合"+dd+"天数据出错:{}",e);
        }
    }

    @Override
    public void getCSVAllday(String dd) {

    }

//    @Override
//    public void setDd() {
//        List<AlarmResult> results = alarmResultDao.selectList(null);
//        for (int i = 0; i < results.size(); i++) {
//            Date lastTime = results.get(i).getLastTime();
//            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//            sdf1.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置为东八区
//            String format = sdf1.format(lastTime);
//            String dateHourPart = format.substring(0, 13);
//// 去除所有非数字字符，得到 "2025082516"
//            String ds = dateHourPart.replaceAll("[^0-9]", "");
//            results.get(i).setDs(ds);
//            LambdaQueryWrapper<AlarmResult> queryWrapper = new LambdaQueryWrapper<>();
//            queryWrapper.eq(AlarmResult::getKey,results.get(i).getKey());
//            queryWrapper.eq(AlarmResult::getLastTime,results.get(i).getLastTime());
//            alarmResultDao.update(results.get(i),queryWrapper);
//            log.info("修改{}条数据",i);
//        }
//    }

    public void insertAllDay(List<String> keys,String dd){
        keys.forEach(item ->{
            String[] split = item.split("_");
            List<AlarmResult> results = alarmResultDao.selectAllAlarmByKey(item,"alarm_result_"+dd);
            AlarmResultAllday alarmResultAllday = new AlarmResultAllday();
            alarmResultAllday.setKey(item);
            alarmResultAllday.setSrcIp(split[0]);
            alarmResultAllday.setDstIp(split[1]);
            alarmResultAllday.setEventName(split[2]);
            alarmResultAllday.setDs(dd);
            alarmResultAllday.setLastTime(results.get(0).getLastTime());
            JSONArray jsonArray = new JSONArray();
            results.forEach( result -> {
                JSONArray array = JSON.parseArray(result.getResult());
                for (int i = 0; i < array.size(); i++) {
                    // 获取当前JSON对象
                    JSONObject jsonObject = array.getJSONObject(i);
                    jsonObject.remove("REQUEST_PAYLOAD");
                    jsonObject.remove("RESPONSE_MESSAGE");
                }
                jsonArray.addAll(array);
            });
            ObjectMapper objectMapper = new ObjectMapper();
            String json = null;
            try {
                json = objectMapper.writeValueAsString(jsonArray);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            alarmResultAllday.setResult(json);
            //log.info("插入一条数据{}",alarmResultAllday);
            alarmResultAlldayDao.insert(alarmResultAllday,"alarm_result_day_"+dd);
        });
    }
}

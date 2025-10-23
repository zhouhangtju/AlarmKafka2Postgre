package com.mobile.outalarm2.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mobile.outalarm2.dao.AlarmResultDao;
import com.mobile.outalarm2.db.AlarmResult;
import com.mobile.outalarm2.db.Business;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.checkerframework.checker.units.qual.A;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Slf4j
@Transactional
public class KafkaConsumer {

    public static final String TOPIC_TEST = "matrix";




    @Autowired
    private AlarmResultDao alarmResultDao;

    @KafkaListener(topics = TOPIC_TEST,  concurrency = "6")
    @Transactional
    public void topic_alarm(List<String> messages, Acknowledgment ack) {
        log.info("获取到kakfa消息条数{},线程{}", messages.size(), Thread.currentThread().getName());
       // System.out.printf("获取到kakfa消息条数");
        Optional message = Optional.ofNullable(messages);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置为东八区
         //   Date now = new Date();
            // 收到信息时的系统时间
          //  String dsNow = sdf.format(DateUtils.addDays(now, 0));
          //  String dsYesterday = sdf.format(DateUtils.addDays(now, -1));
            try {
                if (message.isPresent()) {
                    HashSet<String> dsSet = new HashSet<>();
                    String tableDs = new String();
                    ArrayList<AlarmResult> alarmResults = new ArrayList<>();
                  //  log.info("获取到kakfa消息条数{}", messages.size());
                    for (int i = 0; i < messages.size(); i++) {
                       // Object parse = JSON.parse(messages.get(i));
                        String s = messages.get(i);
//                    Object msg = message.get();
//                    String s = msg.toString();
                    JSONObject jsonObject = JSON.parseObject(s);
                    JSONArray businessJsonList = jsonObject.getJSONArray("businessList");
                    List<Business> businessList = businessJsonList.toJavaList(Business.class);
                    String recong = jsonObject.getString("recong");
                    sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
                    AlarmResult alarmResult = new AlarmResult();
                    String[] split = recong.split("_");
                    //判断同一个批数据会不会跨天
                    //按小时聚合，一天一张表
                    Map<String, List<Business>> collect = businessList.stream().collect(Collectors.groupingBy(Business::getCREATE_TIME));
                    ArrayList<String> timeList = new ArrayList<>();
                 //   ArrayList<String> timeListNewDay = new ArrayList<>();
                    collect.keySet().forEach(key -> {
                        //   String datePart = key.substring(0, 10);
                        String dateHourPart = key.substring(0, 13);
                        // 去除所有非数字字符，得到 "2025082516"
                        String ds = dateHourPart.replaceAll("[^0-9]", "");
                        dsSet.add(ds);
                        timeList.add(key);
                    });
//                    SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                    sdf1.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置为东八区
//                    //dsSet数量大于1，即为跨天
                    List<String> dsList = dsSet.stream().collect(Collectors.toList());
                        tableDs = dsList.get(0).substring(0,8);
//                    if (dsList.size() > 1) {
//                        AlarmResult alarmResultNewDay = new AlarmResult();
//                        ArrayList<Business> businesses = new ArrayList<>();
//                        ArrayList<Business> businessesNewDay = new ArrayList<Business>();
//                        businessList.forEach(item -> {
//                            String createTime = item.getCREATE_TIME();
//                            String datePart = createTime.substring(0, 13);
//                            String ds = datePart.replaceAll("[^0-9]", "");
//                            if (ds.compareTo(dsNow) < 0) {
//                                businesses.add(item);
//                                timeList.add(item.getCREATE_TIME());
//                            } else {
//                                businessesNewDay.add(item);
//                                timeListNewDay.add(item.getCREATE_TIME());
//                            }
//                        });
//                        alarmResult.setDs(dsYesterday);
//                        alarmResult.setLastTime(sdf1.parse(timeList.get(timeList.size() - 1)));
//                        alarmResult.setKey(recong);
//                        alarmResult.setEventName(split[2]);
//                        alarmResult.setSrcIp(split[0]);
//                        alarmResult.setDstIp(split[1]);
//                        alarmResult.setResult(JSONArray.toJSONString(businesses));
//                        alarmResultNewDay.setLastTime(sdf1.parse(timeListNewDay.get(0)));
//                        alarmResultNewDay.setDs(dsNow);
//                        alarmResultNewDay.setKey(recong);
//                        alarmResultNewDay.setEventName(split[2]);
//                        alarmResultNewDay.setSrcIp(split[0]);
//                        alarmResultNewDay.setDstIp(split[1]);
//                        alarmResultNewDay.setResult(JSONArray.toJSONString(businessesNewDay));
//                        alarmResultDao.insert(alarmResult);
//                        alarmResultDao.insert(alarmResultNewDay);
//                    } else {
//                        timeList.sort((item1, item2) -> {
//                            return item1.compareTo(item2);
//                        });
                        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        sdf2.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置为东八区
                        alarmResult.setLastTime(sdf2.parse(timeList.get(0)));
                        alarmResult.setDs(dsList.get(0));
                        alarmResult.setEventName(split[2]);
                        alarmResult.setSrcIp(split[0]);
                        alarmResult.setDstIp(split[1]);
                        alarmResult.setResult(JSON.toJSONString(businessJsonList));
                        alarmResult.setKey(recong);
                        //log.info("收到的数据{}", alarmResult.getKey());
                        alarmResults.add(alarmResult);
//                    }
                  }
                    if(alarmResults.size()!=0){
                        log.info("收到的数据{},{}", alarmResults.get(0).getKey(),alarmResults.get(0).getLastTime());
                    }
                    alarmResultDao.insertBatch(alarmResults,"alarm_result_"+tableDs);
                }
            } catch (Exception e) {
                log.info("消费数据故障:{}", e);
            } finally {
                ack.acknowledge();
            }
        }
    }

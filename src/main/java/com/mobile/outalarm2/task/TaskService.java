package com.mobile.outalarm2.task;



import com.mobile.outalarm2.dao.AlarmResultAlldayDao;
import com.mobile.outalarm2.dao.AlarmResultDao;
import com.mobile.outalarm2.service.AlarmResultService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.apache.commons.lang3.time.DateUtils;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
@Slf4j
public class TaskService{
    private final String taskExecutor = "taskExecutor";

    @Autowired
    private AlarmResultService alarmResultService;
    @Autowired
    private AlarmResultAlldayDao alarmResultAlldayDao;
    @Autowired
    private AlarmResultDao alarmResultDao;


   // @Async("taskExecutor")
//    @Scheduled(cron = "0 0 1 * * ? ") //每天1点执行
//    //@Scheduled(cron = "0 0/1 * * * ? ")         //1分钟执行一次 测试
//    public void getAlarmData() {
//        log.info("====执行读取告警数据库数据任务===");
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置为东八区
//        Date now = new Date();
//        // 格式化并打印日期
//        String dd = sdf.format(DateUtils.addDays(now, -1));
//        alarmResultService.aggregationAllday(dd);
//    }

   // @Async("taskExecutor")
    @Scheduled(cron = "0 0 22 * * ? ") //每天11点执行
    //@Scheduled(cron = "0 0/1 * * * ? ")         //1分钟执行一次 测试
    public void createAlarmAlldayTable() {
        log.info("====执行创建AlarmAllday和AlarmResult数据表的任务===");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置为东八区
        Date now = new Date();
        // 格式化并打印日期
        String dd = sdf.format(DateUtils.addDays(now, 1));
     //   alarmResultAlldayDao.createTable("alarm_result_day_"+dd);
        alarmResultDao.createTable("alarm_result_"+dd);
    }
    @Scheduled(cron = "0 0 23 * * ? ") //每天11点执行
    public void deleteTable(){
        log.info("====执行删除AlarmAllday和AlarmResult数据表的任务===");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置为东八区
        Date now = new Date();
        // 格式化并打印日期
        String dd = sdf.format(DateUtils.addDays(now, -7));
     //   alarmResultAlldayDao.deleteTable("alarm_result_day_"+dd);
        alarmResultDao.deleteTable("alarm_result_"+dd);
    }

}

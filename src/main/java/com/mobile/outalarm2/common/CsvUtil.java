package com.mobile.outalarm2.common;

import com.mobile.outalarm2.db.AlarmResultAllday;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

@Slf4j
public class CsvUtil {



    /**
     * 导出 外呼结果数据库 数据到 CSV 文件，并将文件名按指定日期命名
     * @param list 包含 ResultDB 数据的列表
     * @param dateStr 日期字符串，用于文件命名  格式:20250305
     * @return CSV文件的路径
     */
    public static String exportToCsv(List<AlarmResultAllday> list, String dateStr) {
        // 构建文件路径，包括日期
        //TODO 修改文件路径
        //String fileName = dateStr+"_ExistUsersResult.csv";
        String fileName = dateStr+".csv";
        String directory = "E:\\223333";
        File tmp = new File(directory);
        if (!tmp.exists()) {
            boolean createDir = tmp.mkdir();
            log.info("=== create data directory {}", createDir);
        }


        String path = directory + "/" + fileName;

        File csvFile = new File(path);

        SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        try (PrintWriter writer = new PrintWriter(csvFile)) {
            // 写入标题行
            writer.println("key,result,ds,eventName,srcIp,dstIp,lastTime");
            StringBuilder sb = new StringBuilder(104857600);
            // 遍历数据列表，将数据写入CSV文件
            for (AlarmResultAllday resultDB : list) {
//                writer.println(String.format("%s,%s,%s,%s,%s,%s,%s",
//                        resultDB.getKey(), resultDB.getResult(), resultDB.getDs(),
//                        resultDB.getEventName(), resultDB.getSrcIp(),resultDB.getDstIp(),
//                        dateFormat.format(resultDB.getLastTime())));
                writer.println(sb.append( resultDB.getKey()+","+resultDB.getResult()+","+ resultDB.getDs()+","+
                        resultDB.getEventName()+","+resultDB.getSrcIp()+","+resultDB.getDstIp()+","+
                        dateFormat.format(resultDB.getLastTime())));
                sb.setLength(0);
            }
        } catch (Exception e) {
            log.error("写入CSV文件时出错", e);
            return null;
        }
        log.info("CSV文件已保存到: {}", path);
        return path;
    }
}

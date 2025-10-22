//package com.mobile.outalarm2.kafka;
//
//import cn.hutool.json.JSONUtil;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.support.SendResult;
//import org.springframework.stereotype.Component;
//import org.springframework.util.concurrent.ListenableFuture;
//import org.springframework.util.concurrent.ListenableFutureCallback;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//
///**
// * @author suihao
// * @Title: KafkaProducer
// * @Description TODO
// * @date： 2023/03/03 17:
// * @version： V1.0
// */
//@Component
//@Slf4j
//public class KafkaProducer {
//
//    @Autowired
//    private KafkaTemplate<String, Object> kafkaTemplate;
//
//    //自定义topic
//    public static final String TOPIC_TEST = "matrix";
//    //
//    public static final String TOPIC_GROUP1 = "topic.group1";
//
//
//    public void test() {
//        // 2. 读取TXT文件并发送数据
////        try (BufferedReader reader = new BufferedReader(new FileReader("C:/Users/47480/Desktop/DCN.txt"))) {
////            String line;
////            int count = 0;
////            // 逐行读取文件（假设每行一条JSON数据）
////            while ((line = reader.readLine()) != null) {
////                line = line.trim();
////                if (line.isEmpty()) continue;
////                count++;
//                ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TOPIC_TEST, "123");
//                future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
//                    @Override
//                    public void onFailure(Throwable throwable) {
//                        //发送失败的处理
//                        log.info(TOPIC_TEST + " - 生产者 发送消息失败：" + throwable.getMessage());
//                    }
//
//                    @Override
//                    public void onSuccess(SendResult<String, Object> stringObjectSendResult) {
//                        //成功的处理
//                        log.info(TOPIC_TEST + " - 生产者 发送消息成功：");
//                        System.out.printf("123456");
//                    }
//                });
//            }
//           // log.info("发送了{}条数据",count);
////        } catch (Exception e) {
////            System.err.println("文件处理错误：" + e.getMessage());
////            e.printStackTrace();
////        }
//        //发送消息
////    }
//}
//

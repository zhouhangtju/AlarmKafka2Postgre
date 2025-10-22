package com.mobile.outalarm2.db;

import lombok.Data;

import java.util.List;

@Data
public class Message {
    // 对应 recong=188.104.77.19_188.108.10.106_SSH登录请求认证
    private String recong;

    // 对应 businessList 数组，存储业务明细
    private List<Business> businessList;
}

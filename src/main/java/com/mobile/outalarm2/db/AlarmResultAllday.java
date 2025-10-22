package com.mobile.outalarm2.db;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

@Data
public class AlarmResultAllday {
    private String key;

    private String srcIp;

    private String dstIp;

    private String eventName;

    private String result;

    private String ds;

    private Date lastTime;
}

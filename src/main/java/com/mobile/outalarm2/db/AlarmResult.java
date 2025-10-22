package com.mobile.outalarm2.db;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

@Data
@TableName("alarm_result")
public class AlarmResult {

    private String key;

    @TableField("src_ip")
    private String srcIp;

    @TableField("dst_ip")
    private String dstIp;
    @TableField("event_name")
    private String eventName;

    private String result;

    private String ds;

    @TableField("last_time")
    private Date lastTime;
}

package com.mobile.outalarm2.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mobile.outalarm2.db.AlarmResult;
import com.mobile.outalarm2.db.AlarmResultAllday;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface AlarmResultAlldayDao{


    void createTable(String tableName);


    void insert(AlarmResultAllday alarmResultAllday,String tableName);
}

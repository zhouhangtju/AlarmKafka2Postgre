package com.mobile.outalarm2.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.mobile.outalarm2.db.AlarmResult;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface AlarmResultDao{
       
        void insertBatch(@Param("list") List<AlarmResult> list,@Param("tableName") String tableName);

        void insert(AlarmResult alarmResult,String tableName);

        List<AlarmResult> selectAllKey(String tableName);

        List<AlarmResult> selectAllAlarmByKey(String key,String tableName);

        void createTable(@Param("tableName") String tableName);

        void deleteTable(@Param("tableName") String tableName);
}

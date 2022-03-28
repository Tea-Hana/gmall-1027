package com.atguigu.gmallpublisher.Mapper;

import java.util.List;
import java.util.Map;

/**
 * @Author Hana
 * @Date 2022-03-19-0:35
 * @Description :
 */

public interface DauMapper {
    //获取日活总数数据
    public Integer selectDauTotal(String date);

    //获取分时数据
    public List<Map> selectDauTotalHourMap(String date);
}

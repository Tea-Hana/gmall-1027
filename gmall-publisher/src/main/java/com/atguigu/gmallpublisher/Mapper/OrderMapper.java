package com.atguigu.gmallpublisher.Mapper;

import java.util.List;
import java.util.Map;

/**
 * @Author Hana
 * @Date 2022-03-22-18:24
 * @Description :
 */
public interface OrderMapper {
    //获取当天总数gmv
    public Double selectOrderAmountTotal(String date);

    //获取当天分时数据gmv
    public List<Map> selectOrderAmountHourMap(String date);

}

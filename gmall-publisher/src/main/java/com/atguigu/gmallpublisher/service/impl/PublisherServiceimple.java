package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.Mapper.DauMapper;
import com.atguigu.gmallpublisher.Mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author Hana
 * @Date 2022-03-19-0:41
 * @Description :
 */
@Service
public class PublisherServiceimple implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    @Override
    public Map getDauHourTotal(String date) {
       //1. 获取mapper查询到的数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //创建新的map集合
        HashMap<String, Long> result = new HashMap<>();

        // 2. 取出老map中的(K : LH/CT v:LH/CT 所对应的值)中的数据，封装到新的map
    // (K : LH对应的值，v: CT对应的值)中
        // 注意！！！！ LH和CT必须是大写的，因为从phoenix查询出来就是大写的所以封装到Map也是大写的

    for (Map map : list) {
      result.put((String) map.get("LH"),(Long) map.get("CT"));
    }

        return result;
    }

    @Override
    public Double getGmvTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getGmvHourTotal(String date) {
        //1.获取mapper查询到的数据
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //创建新的map集合
        HashMap<String, Double> result = new HashMap<>();

        // 2. 取出老map中的(K : CREATE_HOUR/SUM_AMOUNT v:CREATE_HOUR/SUM_AMOUNT 所对应的值)中的数据，封装到新的map
        // (K : CREATE_HOUR对应的值，v:SUM_AMOUNT对应的值)中
        // 注意！！！！ CREATE_HOUR和SUM_AMOUNT必须是大写的，因为从phoenix查询出来就是大写的所以封装到Map也是大写的
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"),(Double)map.get("SUM_AMOUNT"));
        }

        return result;
    }
}

package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import lombok.experimental.Tolerate;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.PublicKey;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author Hana
 * @Date 2022-03-19-0:45
 * @Description :
 */

@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){

        //1.获取总数数据
        Integer dauTotal = publisherService.getDauTotal(date);
        //获取交易额总数数据
        Double gmvTotal = publisherService.getGmvTotal(date);

        //2.创建list集合用来存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3. 创建存放新增日活的map 集合
        HashMap<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",dauTotal);


        //4. 创建存放新增设备的map集合
        HashMap<String, Object> devMap = new HashMap<>();
        devMap.put("id","new");
        devMap.put("name","新增设备");
        devMap.put("value","233");

        //5. 创建存放新增交易额的map集合
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id","order_amount");
        gmvMap.put("name","新增交易额");
        gmvMap.put("value",gmvTotal);


        result.add(dauMap);
        result.add(devMap);
        result.add(gmvMap);

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauHourTotal(@RequestParam String id, @RequestParam String date){

        //1.创建Map用来存放结果数据
        HashMap<String, Map> result = new HashMap<>();


        //2. 获取昨天的日期
        String yesterday= LocalDate.parse(date).plusDays(-1).toString();

        Map todayMap = null;

        Map yesterdayMap = null;

        //根据id进行判断，往最终map集合中放的是哪个需求的数据
        if ("dau".equals(id)){
            //3.分别获取昨天的数据和今天的数据
            todayMap = publisherService.getDauHourTotal(date);
            yesterdayMap = publisherService.getDauHourTotal(yesterday);
        }else if ("order_amount".equals(id)){
            todayMap = publisherService.getGmvHourTotal(date);
            yesterdayMap = publisherService.getGmvHourTotal(yesterday);
        }

        //将数据封装到存放结果的集合中
        result.put("yesterday",yesterdayMap);
        result.put("today",todayMap);

        return JSONObject.toJSONString(result);
    }

}

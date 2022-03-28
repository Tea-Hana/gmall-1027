package com.atguigu.gmallpublisher.service;

import org.springframework.stereotype.Service;

import javax.jnlp.DownloadService;
import java.util.Map;

/**
 * @Author Hana
 * @Date 2022-03-19-0:39
 * @Description :
 */
public interface PublisherService {
    //日活总数接口
    public Integer getDauTotal(String date);
    //日活分时数据接口
    public Map getDauHourTotal(String date);

    //交易额总数接口
    public Double getGmvTotal (String date);

    //交易分时数据接口
    public Map getGmvHourTotal (String date);


}

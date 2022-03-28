package com.atguigu.constants;

/**
 * @Author Hana
 * @Date 2022-03-16-16:17
 * @Description :
 */
public class GmallConstants {
    //启动数据主题
    public static final String KAFKA_TOPIC_STARTUP = "GMALL_STARTUP";

    //发送数据主题
    public static final String KAFKA_TOPIC_ORDER = "GMALL_ORDER";

    //事件日志主题
    public static final String KAFKA_TOPIC_EVENT = "GMALL_EVENT";

    //预警需求索引前缀
    public static final String ES_INDEX_ALETR ="gmall_coupon_alert";

    //订单明细主题
    public static final String KAFKA_TOPIC_ORDER_DETAIL = "TOPIC_ORDER_DETAIL";

    //用户数据主题
    public static final String KAFKA_TOPIC_USER_INFO = "TOPIC_USER_INFO";

    //灵活分析需求索引前缀
    public static final String ES_INDEX_SALEDETAIL = "gmall2022_sale_detail";
}


package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

/** @Author Hana @Date 2022-03-22-16:37 @Description : */
public class CanalClient {
  public static void main(String[] args) throws InvalidProtocolBufferException {
    // 1. 创建链接对象
    CanalConnector canalConnector =
        CanalConnectors.newSingleConnector(
            new InetSocketAddress("hadoop102", 11111), "example", "", "");
    while (true) {
      // 2. 获取链接,在死循环中能一直保持链接
      canalConnector.connect();

      // 3. 选择订阅的数据库 注意！！！！括号里面需要的是正则表达式
      canalConnector.subscribe("gmall1027.*");

      // 4. 获取多个sql执行的结果
      Message message = canalConnector.get(100);

      // 5. 获取一个sql执行的结果
      List<CanalEntry.Entry> entries = message.getEntries();

      // 如果没数据的话，休息一会在拉去数据
      if (entries.size() <= 0) {
        try {
          Thread.sleep(5000);
          System.out.println("没有数据休息一会");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        // 有数据
        for (CanalEntry.Entry entry : entries) {
          // TODO 6.获取表名
          String tableName = entry.getHeader().getTableName();

          // 7. 根据entry类型获取序列化数据
          CanalEntry.EntryType entryType = entry.getEntryType();
          if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {

            // 8. 获取序列化数据
            ByteString storeValue = entry.getStoreValue();

            // 9. 对数据做反序列化
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

            // 10. 获取事件类型
            CanalEntry.EventType eventType = rowChange.getEventType();

            // 11. 获取具体的多行数据
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

            // 根据不同的需求获取不同表中不同事件类型（新增/新增及变化/变化/删除）的数据
            handle(tableName, eventType, rowDatasList);
          }
        }
      }
    }
  }

  private static void handle(
      String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
    // 获取订单表中新增的数据
    if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
      // 获取每一行数据
      saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER);
    }else if ("order_detail".equals(tableName)&&CanalEntry.EventType.INSERT.equals(eventType)){
      // 获取每一行数据
      saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
    }else if ("user_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType)){
      saveToKafka(rowDatasList, GmallConstants.KAFKA_TOPIC_USER_INFO);
    }
  }

  private static void saveToKafka(List<CanalEntry.RowData> rowDatasList, String kafkaTopicUserInfo) {
    // 获取每一行数据
    for (CanalEntry.RowData rowData : rowDatasList) {
      // 获取每一行中每一列的数据
      List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
      JSONObject jsonObject = new JSONObject();
      for (CanalEntry.Column column : afterColumnsList) {
        // 获取每一列中值并封装成json格式发送给kafka
        jsonObject.put(column.getName(), column.getValue());
      }
      System.out.println(jsonObject.toJSONString());
      //模拟网络延迟
      try {
        Thread.sleep(new Random().nextInt(5)*1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      MyKafkaSender.send(kafkaTopicUserInfo, jsonObject.toJSONString());
    }
  }
}

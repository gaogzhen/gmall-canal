package com.gaogzhen.config;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.CanalMQConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author gaogzhen
 * @since 2023/9/25 20:05
 */
@Slf4j
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        // 获取canal链接对象
        String ip = "127.0.0.1";
        int port = 11111;
        InetSocketAddress inetSocketAddress = new InetSocketAddress(ip, port);
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(inetSocketAddress, "example", "", "");

        while (true) {
            // 获取连接
            canalConnector.connect();
            // 指定要监控的数据库
            canalConnector.subscribe("gmall-2021.*");
            // 获取Message
            Message message = canalConnector.get(100);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (CollectionUtils.isEmpty(entries)) {
                log.info("没有数据休息一会");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                for (CanalEntry.Entry entry : entries) {
                    String tableName = entry.getHeader().getTableName();
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                        ByteString storeValue = entry.getStoreValue();
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        for (CanalEntry.RowData rowData : rowDatasList) {
                            JSONObject beforeData = new JSONObject();
                            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                            for (CanalEntry.Column column : beforeColumnsList) {
                                beforeData.put(column.getName(), column.getValue());
                            }

                            JSONObject afterData = new JSONObject();
                            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                            for (CanalEntry.Column column : afterColumnsList) {
                                afterData.put(column.getName(), column.getValue());
                            }

                            System.out.println("TableName:" + tableName + ",EventType:" + eventType + ",Before:" + beforeData + ",After:" + afterData);
                        }
                    }
                }
            }
        }
    }
}

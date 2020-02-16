package com.pyg.report.controller;

import com.alibaba.fastjson.JSON;
import com.pyg.report.bean.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

// 表示这是一个Controller，并且其中的(下面)所有方法都是带有@ResponseBody的注解
@RestController
public class ReportController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    // json数据格式
    // RequestBody 可以将Map数据类型自动转为 JSON
    @RequestMapping("/receive")
    public Map<String, String> receive(@RequestBody String json) {

        Map<String, String> map = new HashMap<>();

        try {
            // 构建Message
            Message msg = new Message();
            msg.setMessage(json);
            msg.setCount(1);
            msg.setTimeStamp(System.currentTimeMillis());

            // 转为JSON字符串发送
            String msgJSON = JSON.toJSONString(msg);

            // 发送Message到Kafka
            kafkaTemplate.send("pyg", msgJSON);

            map.put("sucess", "true");
        } catch (Exception e) {
            e.printStackTrace();
            map.put("sucess", "false");
        }

        return map;
    }
}

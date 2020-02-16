package com.pyg.report;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * 在 kafka-manager中创建 test cluster，创建test topic，指定3个分区，2个副本
 *
 * 创建消费者
 *  bin/kafka-console-consumer.sh --zookeeper master:2181 --from-beginning --topic test
 *
 */

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaTest {
    @Autowired
    KafkaTemplate kafkaTemplate;

    @Test
    public void sendMsg() {
        for (int i = 0; i < 100; i ++) {
            // 指定了 key(都是一样的)，所以消息都落到了一台机器上，这是不可行的
            // 1、不指定key，只传递两个参数来解决
            // 2、自定义规则，轮询
            kafkaTemplate.send("test", "key", "this is test msg " + i);
        }
    }
}

package com.jieyw.utils;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * RocketMqProductTest class
 *
 * @author jieYW
 * @date 2018/6/22
 */
public class RocketMqProductTest {
    public static void main(String[] args) throws Exception{
        final DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr("192.168.1.131:9876;192.168.1.132:9876");
        producer.setInstanceName("producer");
        producer.start();
        for (int i = 0; i < 10; i++) {
            Message msg = new Message("order","order_shop","orderId"+i,("dingdan"+1).getBytes());
            SendResult sendResult = producer.send(msg);
            System.out.println(sendResult);
            switch (sendResult.getSendStatus()){
                //消息发送成功
                case SEND_OK:
                    System.out.println("发送成功");
                    break;
                case FLUSH_DISK_TIMEOUT:
                    System.out.println("消息发送成功，但是服务器刷盘超时，消息已经进入服务器队列，只有此时服务器宕机，消息才会丢失");
                    break;
                case FLUSH_SLAVE_TIMEOUT:
                    System.out.println("消息发送成功，但是服务同步到slave时超时，消息已经进入服务器队列，只有此时服务器宕机，消息才会丢失");
                    break;
                case SLAVE_NOT_AVAILABLE:
                    System.out.println("消息发送成功，但是此时slave不可用，消息已经进入服务器队列，只有此时服务器宕机，消息才会丢失");
                    break;
                    //默认
                    default:
                        System.out.println("消息发送失败，存到数据库定时程序重发");

            }
            TimeUnit.MILLISECONDS.sleep(1000);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                producer.shutdown();
            }
        }));
    }
}

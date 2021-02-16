package pers.clare.demo.config;


import io.lettuce.core.event.connection.ConnectedEvent;
import io.lettuce.core.resource.DefaultClientResources;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import pers.clare.core.scheduler.AbstractScheduleMQService;
import pers.clare.redis.MyRedisMessageListenerContainer;
import pers.clare.redis.RedisSubListenerContainer;

import javax.management.monitor.CounterMonitor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Log4j2
@Service
public class ScheduleMQServiceImpl extends AbstractScheduleMQService implements InitializingBean {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    //    @Autowired
//    private MyRedisMessageListenerContainer listenerContainer;
    @Autowired
    private RedisSubListenerContainer listenerContainer;

    @Autowired
    private DefaultClientResources defaultClientResources;

    private long total = 0;
    private Object lock = new Object();
    private long t = System.currentTimeMillis();

    @Override
    public void afterPropertiesSet() throws Exception {
        defaultClientResources.eventBus().get().subscribe((event) -> {
            log.info("---------Event:{}", event);
            if (event instanceof ConnectedEvent) {
                publishConnectedEvent();
            }
        });


//        listenerContainer.addMessageListener((message, pattern) -> {
//            long time = System.currentTimeMillis();
//            synchronized (lock) {
//                if (time - t >= 1000) {
//                    t = time;
//                    log.info("total:{}", total);
//                    total = 0;
//                } else {
//                    total++;
//                }
//            }
//
//        }, "aaaa");
//        }, new ChannelTopic("channel"));

        for (int i = 0; i < 50000; i++) {
            listenerContainer.addMessageListener((message, pattern) -> {
//                log.info("pattern:{},channel:{}", pattern, new String(message.getChannel()));
            }, "pattern." + i);
        }
//        }, new PatternTopic("pattern.*"));

        listenerContainer.addPatternMessageListener((message, pattern) -> {
            long time = System.currentTimeMillis();
            synchronized (lock) {
                if (time - t >= 1000) {
                    t = time;
                    log.info("total:{}", total);
                    total = 0;
                } else {
                    total++;
                }
            }
        }, "pattern.*");

//        new Thread(() -> {
//            try {
//                Thread.sleep(2000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//           MessageListener patternMessageListener = listenerContainer.addPatternMessageListener((message, pattern) -> {
//                assert pattern != null;
//                log.info("pattern:{},channel:{},message:{}", new String(pattern), new String(message.getChannel()), new String(message.getBody()));
//            }, "async1");
////            }, new PatternTopic("async"));
//            MessageListener messageListener = listenerContainer.addMessageListener((message, pattern) -> {
//                assert pattern != null;
//                log.info("pattern:{},channel:{},message:{}", pattern, new String(message.getChannel()), new String(message.getBody()));
//            }, "async2");
//
//            try {
//                Thread.sleep(60000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//            listenerContainer.removePatternMessageListener(patternMessageListener, "async1");
////            }, new PatternTopic("async"));
//            listenerContainer.removeMessageListener(messageListener, "async2");
//        }).start();

        String data = "我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我我";
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int i = 0; i < 20; i++) {
            executorService.execute(() -> {
                int c = 0;
                while (true) {
                    stringRedisTemplate.convertAndSend("pattern." + (c++), data);
                    if (c == 50000) c = 0;
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                }
            });
        }

    }


    @Override
    public void send(String topic, String body) {
        stringRedisTemplate.convertAndSend(topic, body);
    }

    //    @Override
//    public void addListener(String topic, Consumer<String> listener) {
//        myRedisMessageListenerContainer.addMessageListener((message, pattern) -> {
//            log.info("pattern:{},message:{}", new String(pattern), message);
//            listener.accept(new String(message.getBody()));
//        }, new PatternTopic(topic));
//    }
    @Override
    public void addListener(String topic, Consumer<String> listener) {
//        listenerContainer.addMessageListener((message, pattern) -> {
//            log.info("pattern:{},message:{}", new String(pattern), message);
//            listener.accept(new String(message.getBody()));
//        }, topic);
    }

    private int count = 0;

//    @Scheduled(cron = "0/10 * * * * ?")
//    public void test() {
//        stringRedisTemplate.convertAndSend("aaaa", String.valueOf(count++));
//        stringRedisTemplate.convertAndSend("pattern" , String.valueOf(count++));
//        stringRedisTemplate.convertAndSend("async1", String.valueOf(count++));
//        stringRedisTemplate.convertAndSend("async2", String.valueOf(count++));
//    }
}

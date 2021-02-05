package pers.clare.demo.config;


import io.lettuce.core.event.connection.ConnectedEvent;
import io.lettuce.core.resource.DefaultClientResources;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.stereotype.Service;
import pers.clare.core.scheduler.AbstractScheduleMQService;
import pers.clare.redis.MyRedisMessageListenerContainer;

import java.util.function.Consumer;

@Log4j2
@Service
public class ScheduleMQServiceImpl extends AbstractScheduleMQService implements  InitializingBean {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private MyRedisMessageListenerContainer myRedisMessageListenerContainer;

    @Autowired
    private DefaultClientResources defaultClientResources;

    @Override
    public void afterPropertiesSet() throws Exception {
        defaultClientResources.eventBus().get().subscribe((event) -> {
            if (event instanceof ConnectedEvent) {
                publishConnectedEvent();
            }
        });
    }

    @Override
    public void send(String topic, String body) {
        stringRedisTemplate.convertAndSend(topic, body);
    }

    @Override
    public void addListener(String topic, Consumer<String> listener) {
        myRedisMessageListenerContainer.addMessageListener((message, pattern) -> {
            log.info("pattern:{},message:{}", new String(pattern), message);
            listener.accept(new String(message.getBody()));
        }, new PatternTopic(topic));
    }
}

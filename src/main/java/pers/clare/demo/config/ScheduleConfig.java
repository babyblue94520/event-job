package pers.clare.demo.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pers.clare.core.scheduler.*;
import pers.clare.core.scheduler.impl.SchedulerImpl;

import javax.sql.DataSource;
import java.util.HashMap;

@Configuration
public class ScheduleConfig implements InitializingBean {

    @Autowired
    private Scheduler scheduler;

    @Override
    public void afterPropertiesSet() throws Exception {
        for (int i = 0; i < 10; i++) {
            Job job = new Job("Test", "test" + i, "test" + i, "+00:00", "0/1 * * * * ?", true, new HashMap<>());
            // 新增任務
            scheduler.add(job);
            // 註冊任務處理器
            scheduler.register(job.getGroup(), job.getName(), eventJob -> {
                System.out.println(job.getName());
            });
        }

    }

    @Bean
    public Scheduler scheduler(
            @Value("${event-job.name}") String instance
            , @Value("${event-job.topic}") String topic
            , @Qualifier("dataSource") DataSource dataSource
            , EventJobMessageService eventJobMessageService
    ) {
        return new SchedulerImpl(instance, dataSource, topic, eventJobMessageService);
    }
}

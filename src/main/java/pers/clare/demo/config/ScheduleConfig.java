package pers.clare.demo.config;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pers.clare.core.scheduler.*;

import javax.sql.DataSource;
import java.util.HashMap;

@Configuration
public class ScheduleConfig implements InitializingBean {
    @Autowired
    Scheduler scheduler;


    @Override
    public void afterPropertiesSet() throws Exception {
        Job job = new Job("Test", "test", "test", "+00:00", "0/10 * * * * ?", true, new HashMap<>());

        scheduler.handler(job.getGroup(), job.getName(), (eventJob) -> {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        });
        scheduler.add(job);

    }

    @Bean
    public Scheduler scheduler(
            DataSource dataSource
    ) {
        return new JdbcScheduler(dataSource);
    }

}

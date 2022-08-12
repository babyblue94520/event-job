package pers.clare.eventjob;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import pers.clare.eventjob.impl.EventSchedulerImpl;
import pers.clare.eventjob.impl.JdbcJobStoreImpl;

import javax.sql.DataSource;

@Configuration
@ConditionalOnBean(EventJobProperties.class)
public class EventJobAutoConfiguration {

    @Bean
    @Autowired(required = false)
    @ConditionalOnMissingBean(EventScheduler.class)
    public EventScheduler eventScheduler(
            EventJobProperties eventJobProperties
            , JobStore jobStore
            , @Nullable EventJobMessageService eventJobMessageService
    ) {
        return new EventSchedulerImpl(eventJobProperties, jobStore, eventJobMessageService);
    }

    @Bean
    @ConditionalOnMissingBean(JobStore.class)
    public JobStore jobStore(
            DataSource dataSource
    ) {
        return new JdbcJobStoreImpl(dataSource);
    }
}

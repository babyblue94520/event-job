package pers.clare.eventjob;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import pers.clare.eventjob.impl.JdbcJobStoreImpl;
import pers.clare.eventjob.impl.LocalJobStoreImpl;
import pers.clare.eventjob.impl.SchedulerImpl;

import javax.sql.DataSource;

@Configuration
public class EventJobConfiguration {
    private static final String PREFIX = "event-job";
    private static final String DATASOURCE = "eventJobDataSource";
    private static final String DATASOURCE_PROPERTIES = PREFIX + ".datasource";
    private static final String DATASOURCE_URL = PREFIX + ".datasource.url";


    @Bean(name = DATASOURCE)
    @ConfigurationProperties(prefix = DATASOURCE_PROPERTIES)
    @ConditionalOnMissingBean(name = DATASOURCE)
    @ConditionalOnProperty(prefix = "event-job", name = "persistence",havingValue = "true")
    public DataSource eventJobDataSource(
            @Value("${" + DATASOURCE_URL + ":}") String url
    ) {
        return DataSourceBuilder.create()
                .url(url)
                .build();
    }

    @Bean
    @Autowired(required = false)
    public JobStore jobStore(
            @Nullable @Qualifier(DATASOURCE) DataSource dataSource
    ) {
        return dataSource == null ? new LocalJobStoreImpl() : new JdbcJobStoreImpl(dataSource);
    }

    @Bean
    @Autowired(required = false)
    @ConfigurationProperties(PREFIX)
    @ConditionalOnMissingBean(Scheduler.class)
    public Scheduler scheduler(
            JobStore jobStore
            , @Nullable EventJobMessageService eventJobMessageService
    ) {
        return new SchedulerImpl(jobStore, eventJobMessageService);
    }
}

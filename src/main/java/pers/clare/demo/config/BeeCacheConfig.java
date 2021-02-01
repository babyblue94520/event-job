package pers.clare.demo.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import pers.clare.core.cache.BeeCacheMQService;
import pers.clare.core.cache.BeeCacheManager;
import pers.clare.core.cache.busy.BusyBeeCacheManager;
import pers.clare.core.cache.expire.ExpireBeeCacheManager;

@Configuration
public class BeeCacheConfig {
    private static final String PT60S = "PT60S";

    // 闲置 60 秒清除 Cache
    public static final String ExpirePT60SName = "expireBeeCacheManager" + PT60S;
    // 闲置 60 秒清除 Cache
    public static final String BusyPT60SName = "busyBeeCacheManager" + PT60S;

    @Bean
    @Primary
    public CacheManager beeCacheManager(
            @Autowired(required = false) BeeCacheMQService beeCacheMQService
            , @Value("${cache.notify.topic:default}") String topic
    ) {
        return new BeeCacheManager(topic, beeCacheMQService);
    }

    @Bean(ExpirePT60SName)
    public CacheManager ExpirePT1D(
            @Autowired(required = false) BeeCacheMQService beeCacheMQService
            , @Value("${cache.notify.topic:default}") String topic
    ) {
        return new ExpireBeeCacheManager(topic, beeCacheMQService, PT60S);
    }

    @Bean(BusyPT60SName)
    public CacheManager BusyPT1D(
            @Autowired(required = false) BeeCacheMQService beeCacheMQService
            , @Value("${cache.notify.topic:default}") String topic
    ) {
        return new BusyBeeCacheManager(topic, beeCacheMQService, PT60S);
    }
}

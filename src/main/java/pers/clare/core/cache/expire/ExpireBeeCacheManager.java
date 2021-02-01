package pers.clare.core.cache.expire;

import lombok.extern.log4j.Log4j2;
import pers.clare.core.cache.BeeCache;
import pers.clare.core.cache.BeeCacheMQService;
import pers.clare.core.cache.BeeCacheManager;
import pers.clare.core.cache.busy.BusyBeeCache;

import java.time.Duration;

@Log4j2
public class ExpireBeeCacheManager extends BeeCacheManager {
    protected final long effectiveTime;

    public ExpireBeeCacheManager(
            String topic
            , BeeCacheMQService beeCacheMQService
            , String duration
    ) {
        super(topic + ".expire." + duration, beeCacheMQService);
        effectiveTime = Duration.parse(duration).toMillis();
    }

    @Override
    public BeeCache createCache(String name) {
        return new ExpireBeeCache(this, name, effectiveTime);
    }
}

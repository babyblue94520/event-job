package pers.clare.core.cache.busy;

import lombok.extern.log4j.Log4j2;
import pers.clare.core.cache.BeeCache;
import pers.clare.core.cache.BeeCacheMQService;
import pers.clare.core.cache.expire.ExpireBeeCacheManager;

@Log4j2
public class BusyBeeCacheManager extends ExpireBeeCacheManager {

    public BusyBeeCacheManager(
            String topic
            , BeeCacheMQService beeCacheMQService
            , String duration
    ) {
        super(topic + ".busy", beeCacheMQService, duration);
    }

    @Override
    public BeeCache createCache(String name) {
        return new BusyBeeCache(this, name, effectiveTime);
    }
}

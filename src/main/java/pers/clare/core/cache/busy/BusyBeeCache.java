package pers.clare.core.cache.busy;

import lombok.extern.log4j.Log4j2;
import org.springframework.cache.Cache;
import pers.clare.core.cache.BeeCacheManager;
import pers.clare.core.cache.expire.ExpireBeeCacheValueWrapper;
import pers.clare.core.cache.expire.ExpireBeeCache;

@Log4j2
public class BusyBeeCache extends ExpireBeeCache {

    protected BusyBeeCache(BeeCacheManager manager, String name, long effectiveTime) {
        super(manager, name,effectiveTime);
    }

    @Override
    public Cache.ValueWrapper get(Object key) {
        ExpireBeeCacheValueWrapper value = (ExpireBeeCacheValueWrapper) super.get(key);
        if (value == null) return null;
        long now = System.currentTimeMillis();
        if (value.getValidTime() > now) {
            value.setValidTime(now + effectiveTime);
            return value;
        } else {
            store.remove(key);
            return null;
        }
    }
}

package pers.clare.core.cache.expire;

import lombok.extern.log4j.Log4j2;
import org.springframework.cache.Cache;
import pers.clare.core.cache.BasicBeeCache;
import pers.clare.core.cache.BeeCacheManager;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Log4j2
public class ExpireBeeCache extends BasicBeeCache {
    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    protected final long effectiveTime;

    protected ExpireBeeCache(BeeCacheManager manager, String name, long effectiveTime) {
        super(manager, name);
        // 最低時間 1 分鐘
        long delay = 60 * 1000;
        this.effectiveTime = effectiveTime > delay ? effectiveTime : delay;
        executor.scheduleWithFixedDelay(() -> {
            try {
                expire();
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }, delay, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public Cache.ValueWrapper get(Object key) {
        ExpireBeeCacheValueWrapper value = (ExpireBeeCacheValueWrapper) super.get(key);
        if (value == null) return null;
        long now = System.currentTimeMillis();
        if (value.getValidTime() > now) {
            return value;
        } else {
            store.remove(key);
            return null;
        }
    }

    @Override
    protected Cache.ValueWrapper createValueWrapper(Object value) {
        return new ExpireBeeCacheValueWrapper(value, System.currentTimeMillis() + effectiveTime);
    }

    /**
     *
     */
    protected void expire() {
        long oldSize = store.size();
        if (oldSize == 0) return;
        long now = System.currentTimeMillis();
        long t = System.currentTimeMillis();
        for (Map.Entry<String, Cache.ValueWrapper> entry : store.entrySet()) {
            try {
                if (((ExpireBeeCacheValueWrapper) entry.getValue()).getValidTime() < now) {
                    store.remove(entry.getKey());
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
        log.debug("{} > {} expire {}ms", oldSize, store.size(), System.currentTimeMillis() - t);
    }
}

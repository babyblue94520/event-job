package pers.clare.core.cache;

import lombok.extern.log4j.Log4j2;

import java.util.Collections;
import java.util.concurrent.Callable;

@Log4j2
public class TempBeeCache implements BeeCache {
    protected final BeeCacheManager manager;
    protected final String name;

    protected TempBeeCache(
            BeeCacheManager manager
            , String name
    ) {
        this.name = name;
        this.manager = manager;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getNativeCache() {
        return Collections.EMPTY_MAP;
    }

    @Override
    public ValueWrapper get(Object key) {
        return null;
    }

    @Override
    public <T> T get(Object key, Class<T> type) {
        return null;
    }

    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        return null;
    }

    @Override
    public void put(Object key, Object value) {
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, Object value) {
        return null;
    }

    @Override
    public Object getValue(String key) {
        return null;
    }

    /**
     * 清除資料並發出通知
     *
     * @param key
     */
    @Override
    public void evict(Object key) {
        evictNotify(String.valueOf(key));
    }

    /**
     * 只清除資料
     *
     * @param key
     */
    public void onlyEvict(String key) {
    }

    public void evictNotify(String key) {
        try {
            manager.clearNotify(name, key);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    /**
     * 清除資料並發出通知
     */
    @Override
    public void clear() {
        try {
            manager.clearNotify(name);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    /**
     * 只清除資料
     */
    public void onlyClear() {
    }
}

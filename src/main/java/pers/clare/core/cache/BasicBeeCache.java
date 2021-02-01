package pers.clare.core.cache;

import io.swagger.models.auth.In;
import lombok.extern.log4j.Log4j2;
import org.springframework.cache.Cache;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.regex.Pattern;

@Log4j2
public class BasicBeeCache implements BeeCache {
    protected final ConcurrentMap<String, Cache.ValueWrapper> store = new ConcurrentHashMap<>();
    protected final BeeCacheManager manager;
    protected final String name;

    protected final Function<String, Object> refreshWhenEvictHandler;
    protected final Function<Set<String>, Map<String, Object>> refreshWhenClearHandler;

    protected BasicBeeCache(
            BeeCacheManager manager
            , String name
    ) {
        this.name = name;
        this.manager = manager;
        this.refreshWhenEvictHandler = manager.refreshWhenEvictHandlers.get(name);
        this.refreshWhenClearHandler = manager.refreshWhenClearHandlers.get(name);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getNativeCache() {
        return Collections.unmodifiableMap(store);
    }

    @Override
    public Cache.ValueWrapper get(Object key) {
        return store.get(String.valueOf(key));
    }

    @Override
    public <T> T get(Object key, Class<T> type) {
        return (T) getValue(String.valueOf(key));
    }

    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        return (T) getValue(String.valueOf(key));
    }

    @Override
    public void put(Object key, Object value) {
        store.put(String.valueOf(key), createValueWrapper(value));
    }

    @Override
    public Cache.ValueWrapper putIfAbsent(Object key, Object value) {
        return store.putIfAbsent(String.valueOf(key), createValueWrapper(value));
    }

    public Object getValue(String key) {
        Cache.ValueWrapper value = get(key);
        if (value == null) return null;
        return value.get();
    }

    protected Cache.ValueWrapper createValueWrapper(Object value) {
        return new BeeCacheValueWrapper(value);
    }

    /**
     * 清除資料並發出通知
     *
     * @param key
     */
    @Override
    public void evict(Object key) {
        String str = String.valueOf(key);
        doEvict(str);
        evictNotify(str);
    }

    /**
     * 只清除資料
     *
     * @param key
     */
    public void onlyEvict(String key) {
        if (key == null) return;
        doEvict(key);
    }

    public void evictNotify(String key) {
        try {
            manager.clearNotify(name, key);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void doEvict(String key) {
        if (key.length() > 6) {
            char[] cs = key.toCharArray();
            // 檢查開頭是否為 "regex:"
            if (cs[0] == 'r' && cs[1] == 'e' && cs[2] == 'g' && cs[3] == 'e' && cs[4] == 'x' && cs[5] == ':') {
                Pattern pattern = Pattern.compile(new String(cs, 6, cs.length - 6));
                for (Object k : store.keySet()) {
                    String str = k.toString();
                    if (pattern.matcher(str).find()) {
                        remove(str);
                    }
                }
                return;
            }
        }
        remove(key);
    }

    protected void remove(String key) {
        if (refreshWhenEvictHandler == null) {
            store.remove(key);
            log.debug("evict name:{} key:{}", name, key);
        } else {
            Object value = refreshWhenEvictHandler.apply(key);
            if (value == null) {
                store.remove(key);
            } else {
                store.put(key, createValueWrapper(value));
            }
            log.debug("evict refresh name:{} key:{}", name, key);
        }
        manager.clearDependents(name, key);
    }

    /**
     * 清除資料並發出通知
     */
    @Override
    public void clear() {
        doClear();
        // 不重複清除自己發出的通知
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
        // 不重複清除自己發出的通知
        doClear();
    }

    private void doClear() {
        if (refreshWhenClearHandler == null) {
            store.clear();
            log.debug("clear name:{}", name);
        } else if (store.size() > 0) {
            Set<String> keys = store.keySet();
            Map<String, Object> values = refreshWhenClearHandler.apply(keys);
            if (values == null || values.size() == 0) {
                store.clear();
            } else {
                Map.Entry<String, ValueWrapper> entry;
                Object value;
                for (Iterator<Map.Entry<String, ValueWrapper>> iterator = store.entrySet().iterator(); iterator.hasNext(); ) {
                    entry = iterator.next();
                    value = values.get(entry.getKey());
                    if (value == null) {
                        iterator.remove();
                    } else {
                        entry.setValue(createValueWrapper(value));
                    }
                }
            }
            log.debug("clear refresh name:{}", name);
        }
        manager.clearDependents(name);
    }
}

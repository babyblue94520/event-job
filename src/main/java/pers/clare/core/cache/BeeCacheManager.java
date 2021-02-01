package pers.clare.core.cache;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.CommandLineRunner;
import org.springframework.cache.CacheManager;
import pers.clare.core.lock.IdLock;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

@Log4j2
public class BeeCacheManager implements CacheManager, CommandLineRunner, InitializingBean {
    protected static String id;
    protected static char[] idChars;
    protected static final char initSplit = ':';
    protected static final char idSplit = '@';
    protected static final char eventSplit = ',';
    protected static final int idLength = 13;
    protected static final int eventIndex = 14;

    private static final ConcurrentMap<String, BeeCache> tempMap = new ConcurrentHashMap<>(16);

    protected static final ConcurrentMap<String, BeeCache> cacheMap = new ConcurrentHashMap<>(16);

    protected static final ConcurrentMap<String, Function<String, Object>> refreshWhenEvictHandlers = new ConcurrentHashMap<>(16);

    protected static final ConcurrentMap<String, Function<Set<String>, Map<String, Object>>> refreshWhenClearHandlers = new ConcurrentHashMap<>(16);

    private static Runnable connectedHandler;

    public static void refreshWhenEvict(String cacheName, Function<String, Object> handler) {
        if (refreshWhenEvictHandlers.put(cacheName, handler) != null) {
            throw new RuntimeException(String.format("%s evict refresh handler already exists", cacheName));
        }
    }

    public static void refreshWhenClear(String cacheName, Function<Set<String>, Map<String, Object>> handler) {
        if (refreshWhenClearHandlers.put(cacheName, handler) != null) {
            throw new RuntimeException(String.format("%s clear refresh handler already exists", cacheName));
        }
    }

    protected final String topic;

    protected final IdLock<Object> locks = new IdLock<>() {
    };

    private final BeeCacheMQService beeCacheMQService;

    {
        BeeCacheContext.setManager(this);
    }

    public BeeCacheManager(String topic, BeeCacheMQService beeCacheMQService) {
        this.topic = topic;
        this.beeCacheMQService = beeCacheMQService;
    }

    public void afterPropertiesSet() {
        if (beeCacheMQService == null) return;
        beeCacheMQService.addListener(topic, (data) -> {
            try {
                parse(data);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        });
        if (connectedHandler == null) {
            connectedHandler = () -> {
                cacheMap.values().forEach(BeeCache::onlyClear);
                tempMap.values().forEach(BeeCache::onlyClear);
            };
            beeCacheMQService.onConnected(connectedHandler);
        }
    }

    @Override
    public void run(String... args) {
        if (beeCacheMQService == null) return;
        if (id == null) {
            resetId();
            ask();
        }
    }

    private void resetId() {
        id = String.valueOf(System.currentTimeMillis());
        idChars = id.toCharArray();
    }

    /**
     * 解析收到信息
     *
     * @param data
     */
    private void parse(String data) {
        log.debug(data);
        char[] cs = data.toCharArray();
        if (id != null && cs.length > eventIndex) {
            switch (cs[idLength]) {
                case initSplit:
                    if (isMe(cs)) return;
                    switch (cs[eventIndex]) {
                        // 處理發問
                        case BeeAction.ASK:
                            replay();
                            return;
                        // 處理回答
                        case BeeAction.REPLY:
                            if (isSame(cs, 15)) {
                                resetId();
                                ask();
                            }
                            return;
                    }
                    break;
                case idSplit:
                    if (isMe(cs)) return;
                    parseEvictEvent(cs, eventIndex);
                    return;
            }
        }
        // 向下相容
        parseEvictEvent(cs, 0);
    }

    /**
     * 檢查ID是否相同
     *
     * @param cs
     * @param offset
     * @return
     */
    private boolean isSame(char[] cs, int offset) {
        for (int i = 0; i < idLength; i++) {
            if (idChars[i] != cs[i + offset]) return false;
        }
        return true;
    }

    /**
     * 檢查是否是自己發送
     *
     * @param cs
     * @return
     */
    private boolean isMe(char[] cs) {
        for (int i = 0; i < idLength; i++) {
            if (idChars[i] != cs[i]) return false;
        }
        return true;
    }

    /**
     * 詢問所有服務ID，來解決ID衝突
     */
    private void ask() {
        beeCacheMQService.send(topic, id + initSplit + BeeAction.ASK);
    }

    /**
     * 回復ID
     */
    private void replay() {
        beeCacheMQService.send(topic, id + initSplit + BeeAction.REPLY + id);
    }

    /**
     * @param cs
     * @param offset
     */
    private void parseEvictEvent(char[] cs, int offset) {
        int l = cs.length;
        if (l == offset) {
            onlyClear();
        } else {
            String name = null;
            char c;
            int i = offset;
            for (; i < l; i++) {
                c = cs[i];
                if (c == eventSplit) {
                    name = new String(cs, offset, i - offset);
                    offset = ++i;
                    break;
                }
            }
            if (name == null) {
                name = new String(cs, offset, l - offset);
                offset++;
            }
            if (i == l) {
                onlyClear(name);
            } else {
                onlyClear(name, new String(cs, offset, l - offset));
            }
        }
    }


    /**
     * 清除並發布通知
     *
     * @param name
     * @param key
     */
    void clearNotify(
            String name
            , String key
    ) {
        if (beeCacheMQService == null) return;
        beeCacheMQService.send(topic, id + idSplit + name + eventSplit + key);
    }

    /**
     * 清除並發布通知
     *
     * @param name
     */
    void clearNotify(
            String name
    ) {
        if (beeCacheMQService == null) return;
        beeCacheMQService.send(topic, id + idSplit + name);
    }

    /**
     * 清除全部並發布通知
     */
    void clearNotify() {
        if (beeCacheMQService == null) return;
        beeCacheMQService.send(topic, id + idSplit);
    }

    @Override
    public BeeCache getCache(String name) {
        BeeCache cache = cacheMap.get(name);
        if (cache != null) return cache;
        synchronized (locks.getLock(name)) {
            cache = cacheMap.get(name);
            if (cache != null) return cache;
            cache = createCache(name);
            cacheMap.put(name, cache);
            tempMap.remove(name);
        }
        return cache;
    }

    /**
     * cache 可能会有不同的 manager 建构
     * 所以非 Spring Aop 使用时，需建立 temp cache 来执行方法
     * 避免影响原有的使用
     *
     * @param name
     * @return
     */
    public BeeCache getCacheOrTemp(String name) {
        BeeCache cache = cacheMap.get(name);
        if (cache != null) return cache;
        return getTemp(name);
    }

    private BeeCache getTemp(String name) {
        BeeCache cache = tempMap.get(name);
        if (cache != null) return cache;
        cache = new TempBeeCache(this, name);
        tempMap.put(name, cache);
        return cache;
    }

    protected BeeCache createCache(String name) {
        return new BasicBeeCache(this, name);
    }

    @Override
    public Collection<String> getCacheNames() {
        return Collections.unmodifiableSet(this.cacheMap.keySet());
    }

    /**
     * 純粹清除
     *
     * @param name
     * @param key
     */
    public void clear(
            String name
            , String key
    ) {
        if (name == null || key == null) return;
        getCacheOrTemp(name).evict(key);
    }

    /**
     * @param name
     * @param key
     */
    public void onlyClear(
            String name
            , String key
    ) {
        if (name == null || key == null) return;
        getCacheOrTemp(name).onlyEvict(key);
        log.debug("clear {} {}", name, key);
    }

    /**
     * 清除資料並發出通知
     *
     * @param name
     */
    public void clear(
            String name
    ) {
        if (name == null) return;
        getCacheOrTemp(name).clear();
    }

    /**
     * 只清除資料
     *
     * @param name
     */
    public void onlyClear(
            String name
    ) {
        if (name == null) return;
        getCacheOrTemp(name).onlyClear();
        log.debug("clear {}", name);
    }

    /**
     * 純粹清除全部
     */
    public void clear() {
        onlyClear();
        clearNotify();
    }

    /**
     * 純粹清除全部
     */
    public void onlyClear() {
        Collection<String> names = getCacheNames();
        for (String name : names) {
            getCacheOrTemp(name).onlyClear();
        }
    }

    /**
     * 清除相依的緩存
     */
    void clearDependents(
            String name
    ) {
        Collection<BeeCacheDepend> dependents = BeeCacheDependencies.find(name);
        if (dependents == null) {
            return;
        }
        BeeCache cache;
        for (BeeCacheDepend dependent : dependents) {
            cache = getCacheOrTemp(dependent.getName());
            if (cache == null) continue;
            cache.onlyClear();
        }
    }

    /**
     * 清除相依的緩存
     */
    void clearDependents(
            String name
            , String key
    ) {
        Collection<BeeCacheDepend> dependents = BeeCacheDependencies.find(name);
        if (dependents == null) {
            return;
        }
        BeeCache cache;
        for (BeeCacheDepend dependent : dependents) {
            cache = getCacheOrTemp(dependent.getName());
            if (cache == null) continue;
            if (dependent.getAllEntries()) {
                cache.onlyClear();
            } else {
                cache.onlyEvict(key);
            }
        }
    }
}

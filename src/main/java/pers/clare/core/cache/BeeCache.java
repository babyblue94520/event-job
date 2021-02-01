package pers.clare.core.cache;

import org.springframework.cache.Cache;

public interface BeeCache extends Cache {

    Object getValue(String key);

    void onlyEvict(String key);

    void onlyClear();
}

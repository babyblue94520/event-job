package pers.clare.demo.controller;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import pers.clare.core.cache.BeeCacheManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "cache")
public class CacheController {
    @Autowired
    private CacheManager cacheManager;

    @GetMapping("name")
    public List<CacheResult> findAllName() {
        List<CacheResult> result = new ArrayList<>();
        Collection<String> names = cacheManager.getCacheNames();
        for (String name : names) {
            Cache cache = cacheManager.getCache(name);
            if (cache == null) {
                result.add(new CacheResult(name, null));
            } else {
                result.add(new CacheResult(name, ((Map<Object, Object>) cache.getNativeCache()).size()));
            }
        }
        return result;
    }

    @GetMapping
    public Object findAll(
            String name
    ) {
        return cacheManager.getCache(name);
    }

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping
    public void clear(
            @RequestParam(required = false) String name
            , @RequestParam(required = false) String key
            , @RequestParam(required = false, defaultValue = "false") Boolean notify
    ) {
        if (notify) {
            if (name == null) {
                ((BeeCacheManager)cacheManager).onlyClear();
            } else {
                if (key == null) {
                    ((BeeCacheManager)cacheManager).clear(name);
                } else {
                    ((BeeCacheManager)cacheManager).onlyClear(name, key);
                }
            }
        } else {
            if (name == null) {
                ((BeeCacheManager)cacheManager).onlyClear();
            } else {
                if (key == null) {
                    ((BeeCacheManager)cacheManager).onlyClear(name);
                } else {
                    ((BeeCacheManager)cacheManager).onlyClear(name, key);
                }
            }
        }
    }

    @Getter
    @AllArgsConstructor
    public class CacheResult {
        private String name;
        private Integer size;
    }
}

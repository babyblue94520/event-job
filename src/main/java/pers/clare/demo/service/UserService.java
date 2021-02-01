package pers.clare.demo.service;

import org.springframework.aop.framework.AopContext;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import pers.clare.core.cache.BeeCacheManager;
import pers.clare.demo.data.User;

import static pers.clare.demo.config.CacheKeyAware.UserCacheKey;
import static pers.clare.demo.config.CacheKeyAware.UserManager;

@Service
public class UserService {

    {
        BeeCacheManager.refreshWhenEvict(UserCacheKey, (key) -> find(Integer.valueOf(key)));
    }

    @Cacheable(
            cacheNames = UserCacheKey
            , cacheManager = UserManager
            , key = "#id"
            , condition = "#id != null"
            , unless = "#result==null"
    )
    public User find(Integer id) {
        return new User(id, "user" + id, 0);
    }

    @CachePut(
            cacheNames = UserCacheKey
            , cacheManager = UserManager
            , key = "#id"
            , condition = "#id != null"
            , unless = "#result==null"
    )
    public User update(Integer id) {
        User user = ((UserService) AopContext.currentProxy()).find(id);
        if (user == null) {
            return null;
        }
        user.setCount(user.getCount() + 1);
        return user;
    }

    @CacheEvict(
            cacheNames = UserCacheKey
            , cacheManager = UserManager
            , key = "#id"
            , condition = "#id != null"
    )
    public void delete(Integer id) {

    }
}

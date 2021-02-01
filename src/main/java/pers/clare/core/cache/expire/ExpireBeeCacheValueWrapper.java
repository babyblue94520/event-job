package pers.clare.core.cache.expire;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.cache.Cache;

@Getter
@AllArgsConstructor
public class ExpireBeeCacheValueWrapper implements Cache.ValueWrapper {
    private final Object value;
    @Setter
    private long validTime;

    @Override
    public Object get() {
        return value;
    }
}

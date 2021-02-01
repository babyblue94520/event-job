package pers.clare.core.cache;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.cache.Cache;

@Getter
@AllArgsConstructor
public class BeeCacheValueWrapper implements Cache.ValueWrapper {
    private final Object value;

    @Override
    public Object get() {
        return value;
    }
}

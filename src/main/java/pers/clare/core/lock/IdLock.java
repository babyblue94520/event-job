package pers.clare.core.lock;

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * 依 ID 建立鎖
 */
public abstract class IdLock<T> {
    // ID 鎖
    private Map<Object, T> locks = new HashMap<>();

    private Class<T> clazz;
    private Map<Integer, Constructor<T>> constructors = new HashMap<>();

    {
        Type type = this.getClass().getGenericSuperclass();
        if (type instanceof ParameterizedType) {
            clazz = (Class<T>) ((ParameterizedType) type).getActualTypeArguments()[0];
        } else {
            clazz = (Class<T>) Object.class;
        }

        for (Constructor constructor : clazz.getDeclaredConstructors()) {
            constructors.put(constructor.getParameterCount(), constructor);
        }
    }

    /**
     * get lock object by id
     *
     * @param id
     * @return
     */
    public T getLock(Object id, Object... parameters) {
        T lock = locks.get(id);
        if (lock != null) return lock;
        synchronized (locks) {
            lock = locks.get(id);
            if (lock != null) return lock;
            locks.put(id, lock = newInstance(parameters));
        }
        return lock;
    }

    public T remove(Object id) {
        synchronized (locks) {
            return locks.remove(id);
        }
    }

    protected T newInstance(Object[] parameters) {
        try {
            return constructors.get(parameters.length).newInstance(parameters);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

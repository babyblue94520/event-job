package pers.clare.core.cache;

import java.util.*;

public abstract class BeeCacheDependencies {

    private static final Map<String, Set<BeeCacheDepend>> depends = new HashMap<>();

    public static Collection<BeeCacheDepend> find(String name) {
        return depends.get(name);
    }

    public static String depend(String name, boolean allEntries, String... dependNames) {
        if (name == null || dependNames == null || dependNames.length == 0) {
            return name;
        }
        Set<BeeCacheDepend> set;
        for (String depend : dependNames) {
            set = depends.get(depend);
            if (set == null) {
                depends.put(depend, (set = new HashSet<>()));
            }
            set.add(new BeeCacheDepend(name, allEntries));
        }
        return name;
    }
}

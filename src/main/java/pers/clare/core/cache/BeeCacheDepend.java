package pers.clare.core.cache;

public class BeeCacheDepend {
    private String name;
    private Boolean allEntries;

    public BeeCacheDepend(String name, Boolean allEntries) {
        this.name = name;
        this.allEntries = allEntries;
    }

    public String getName() {
        return name;
    }

    public Boolean getAllEntries() {
        return allEntries;
    }
}

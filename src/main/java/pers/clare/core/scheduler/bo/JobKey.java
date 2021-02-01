package pers.clare.core.scheduler.bo;

import lombok.Getter;

import java.util.Objects;

@Getter
public class JobKey {
    private String group;
    private String name;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JobKey)) return false;
        JobKey jobKey = (JobKey) o;
        return Objects.equals(group, jobKey.group) &&
                Objects.equals(name, jobKey.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, name);
    }
}

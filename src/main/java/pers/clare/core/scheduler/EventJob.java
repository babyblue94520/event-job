package pers.clare.core.scheduler;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Objects;

@Getter
@AllArgsConstructor
public class EventJob {
    private String instance;
    private String group;
    private String name;
    private String timezone;
    private String description;
    private Boolean concurrent;
    private String cron;
    @Setter(AccessLevel.PACKAGE)
    private Long prevTime;
    @Setter(AccessLevel.PACKAGE)
    private Long nextTime;
    private Boolean enabled;
    private Map<String, Object> data;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventJob)) return false;
        EventJob that = (EventJob) o;
        return Objects.equals(instance, that.instance) && Objects.equals(group, that.group) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instance, group, name);
    }
}

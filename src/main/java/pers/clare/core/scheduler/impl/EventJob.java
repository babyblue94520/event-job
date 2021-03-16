package pers.clare.core.scheduler.impl;

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
    private String cron;
    private Integer status;
    @Setter(AccessLevel.PACKAGE)
    private Long prevTime;
    @Setter(AccessLevel.PACKAGE)
    private Long nextTime;
    @Setter(AccessLevel.PACKAGE)
    private Long startTime;
    @Setter(AccessLevel.PACKAGE)
    private Long endTime;
    private Boolean enabled;
    private Map<String, Object> data;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventJob)) return false;
        EventJob eventJob = (EventJob) o;
        return Objects.equals(instance, eventJob.instance)
                && Objects.equals(group, eventJob.group)
                && Objects.equals(name, eventJob.name)
                && Objects.equals(timezone, eventJob.timezone)
                && Objects.equals(description, eventJob.description)
                && Objects.equals(cron, eventJob.cron)
                && Objects.equals(enabled, eventJob.enabled)
                && Objects.equals(data, eventJob.data)
                ;
    }

    @Override
    public int hashCode() {
        return Objects.hash(instance, group, name, timezone, description, cron, enabled, data);
    }
}

package pers.clare.eventjob.impl;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("UnusedAssignment")
@Getter
@Builder(toBuilder = true)
public class EventJob {
    @NonNull
    private String group;
    @NonNull
    private String name;
    @NonNull
    private String event;
    @NonNull
    private String timezone;
    @NonNull
    @Builder.Default
    private String description = "";
    @NonNull
    @Builder.Default
    private String cron = "";
    @NonNull
    @Builder.Default
    private String afterGroup = "";
    @NonNull
    @Builder.Default
    private String afterName = "";
    @NonNull
    @Builder.Default
    private Boolean enabled = true;
    @NonNull
    @Builder.Default
    private Map<String, Object> data = Collections.emptyMap();

    @Override
    public String toString() {
        return "EventJob{" +
                "group=\"" + group + '\"' +
                ", name=\"" + name + '\"' +
                ", event=\"" + event + '\"' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventJob)) return false;
        EventJob eventJob = (EventJob) o;
        return Objects.equals(group, eventJob.group) && Objects.equals(name, eventJob.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, name);
    }
}

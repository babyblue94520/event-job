package pers.clare.eventjob.vo;

import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

@Getter
@SuperBuilder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class EventJob extends EventJobKey {
    @NonNull
    private String group;
    @NonNull
    private String name;
    @NonNull
    private String event;
    @NonNull
    @Builder.Default
    private String timezone = TimeZone.getDefault().getID();
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

package pers.clare.eventjob.vo;

import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Objects;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder(toBuilder = true)
public class EventJobKey {
    @NonNull
    private String group;
    @NonNull
    private String name;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventJobKey)) return false;
        EventJobKey eventJob = (EventJobKey) o;
        return Objects.equals(group, eventJob.group) && Objects.equals(name, eventJob.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, name);
    }
}

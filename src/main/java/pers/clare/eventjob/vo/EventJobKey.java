package pers.clare.eventjob.vo;

import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Objects;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder(toBuilder = true)
public class EventJobKey {
    private String group;
    private String name;

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof EventJobKey)) return false;

        EventJobKey that = (EventJobKey) o;
        return Objects.equals(getGroup(), that.getGroup()) && Objects.equals(getName(), that.getName());
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(getGroup());
        result = 31 * result + Objects.hashCode(getName());
        return result;
    }
}

package pers.clare.core.scheduler;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;
import java.util.Objects;

@Getter
@AllArgsConstructor
public class Job {
    private String group;
    private String name;
    private String description;
    private String timezone;
    private String cron;
    private Boolean enabled;
    private Map<String, String> data;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Job)) return false;
        Job job = (Job) o;
        return Objects.equals(group, job.group) && Objects.equals(name, job.name) && Objects.equals(description, job.description) && Objects.equals(timezone, job.timezone) && Objects.equals(cron, job.cron) && Objects.equals(enabled, job.enabled) && Objects.equals(data, job.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, name, description, timezone, cron, enabled, data);
    }
}

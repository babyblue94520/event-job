package pers.clare.core.scheduler;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Objects;

@Getter
@Setter
@AllArgsConstructor
public class Job {
    private String group;
    private String name;
    private String description;
    private String timezone;
    private String cron;
    private Boolean enabled;
    private Map<String, Object> data;
}

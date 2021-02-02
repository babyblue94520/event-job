package pers.clare.core.scheduler.bo;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public class Job {
    private String group;
    private String name;
    private String description;
    private Boolean concurrent;
    private String timezone;
    private String cron;
    private Map<String, String> data;
}

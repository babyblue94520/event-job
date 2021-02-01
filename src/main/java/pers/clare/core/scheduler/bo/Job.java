package pers.clare.core.scheduler.bo;

import lombok.Getter;

import java.util.Map;

@Getter
public class Job extends JobKey{
    private String description = "";
    private Boolean concurrent = true;
    private String timezone = "";
    private String cron = "";
    private Map<String, String> data;
}

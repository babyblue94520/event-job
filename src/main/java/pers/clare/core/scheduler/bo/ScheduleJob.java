package pers.clare.core.scheduler.bo;

import lombok.Getter;

import java.util.Map;

@Getter
public class ScheduleJob {
    private String instance;
    private String group;
    private String name;
    private String description = "";
    private Boolean concurrent = true;
    private String timezone = "";
    private String cron = "";
    private Integer status = 0;
    private Long prevTime = 0L;
    private Long nextTime = 0L;
    private Long updateTime;
    private Map<String, String> data;
}

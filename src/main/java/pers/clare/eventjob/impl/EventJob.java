package pers.clare.eventjob.impl;

import java.util.Map;
import java.util.Objects;

public class EventJob {
    private String instance;
    private String group;
    private String name;
    private String event;
    private String timezone;
    private String description;
    private String cron;
    private Integer status;
    private Long prevTime;
    private Long nextTime;
    private Long startTime;
    private Long endTime;
    private Boolean enabled;
    private Map<String, Object> data;

    public EventJob(String instance, String group, String name, String event, String timezone, String description, String cron, Integer status, Long prevTime, Long nextTime, Long startTime, Long endTime, Boolean enabled, Map<String, Object> data) {
        this.instance = instance;
        this.group = group;
        this.name = name;
        this.event = event;
        this.timezone = timezone;
        this.description = description;
        this.cron = cron;
        this.status = status;
        this.prevTime = prevTime;
        this.nextTime = nextTime;
        this.startTime = startTime;
        this.endTime = endTime;
        this.enabled = enabled;
        this.data = data;
    }

    public String getInstance() {
        return instance;
    }

    public String getGroup() {
        return group;
    }

    public String getName() {
        return name;
    }

    public String getEvent() {
        return event;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getDescription() {
        return description;
    }

    public String getCron() {
        return cron;
    }

    public Integer getStatus() {
        return status;
    }

    public Long getPrevTime() {
        return prevTime;
    }

    public Long getNextTime() {
        return nextTime;
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public Map<String, Object> getData() {
        return data;
    }

    void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    void setDescription(String description) {
        this.description = description;
    }

    void setCron(String cron) {
        this.cron = cron;
    }

    void setPrevTime(Long prevTime) {
        this.prevTime = prevTime;
    }

    void setNextTime(Long nextTime) {
        this.nextTime = nextTime;
    }

    void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    void setData(Map<String, Object> data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventJob)) return false;
        EventJob eventJob = (EventJob) o;
        return Objects.equals(instance, eventJob.instance) && Objects.equals(group, eventJob.group) && Objects.equals(name, eventJob.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instance, group, name);
    }
}

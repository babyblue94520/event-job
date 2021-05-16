package pers.clare.eventjob;

import java.util.Map;

public class Job {
    private String group;
    private String name;
    private String event;
    private String description;
    private String timezone;
    private String cron;
    private Boolean enabled;
    private Map<String, Object> data;

    public Job() {

    }

    public Job(String group, String name, String event, String description, String timezone, String cron, Boolean enabled, Map<String, Object> data) {
        this.group = group;
        this.name = name;
        this.event = event;
        this.description = description;
        this.timezone = timezone;
        this.cron = cron;
        this.enabled = enabled;
        this.data = data;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
}

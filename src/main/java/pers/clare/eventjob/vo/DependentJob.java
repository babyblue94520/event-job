package pers.clare.eventjob.vo;


public class DependentJob {
    private final String cron;
    private final String timezone;

    private final String afterGroup;

    private final String afterName;

    public DependentJob(String cron, String timezone, String afterGroup, String afterName) {
        this.cron = cron;
        this.timezone = timezone;
        this.afterGroup = afterGroup;
        this.afterName = afterName;
    }

    public String getCron() {
        return cron;
    }

    public String getTimezone() {
        return timezone;
    }

    public String getAfterGroup() {
        return afterGroup;
    }

    public String getAfterName() {
        return afterName;
    }
}

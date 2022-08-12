package pers.clare.eventjob;

public class JobStatus {
    private final Integer status;

    private final Long nextTime;

    private final Boolean enabled;

    public JobStatus(Integer status, Long nextTime, Boolean enabled) {
        this.status = status;
        this.nextTime = nextTime;
        this.enabled = enabled;
    }

    public Integer getStatus() {
        return status;
    }

    public Long getNextTime() {
        return nextTime;
    }

    public Boolean getEnabled() {
        return enabled;
    }
}

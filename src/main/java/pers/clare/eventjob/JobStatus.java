package pers.clare.eventjob;

import lombok.Getter;

@Getter
public class JobStatus {
    private final Integer status;

    private final Long nextTime;

    private final Long lastActiveTime;

    private final Boolean enabled;

    public JobStatus(Integer status, Long nextTime, Long lastActiveTime, Boolean enabled) {
        this.status = status;
        this.nextTime = nextTime;
        this.lastActiveTime = lastActiveTime;
        this.enabled = enabled;
    }

}

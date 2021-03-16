package pers.clare.core.scheduler.impl;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import pers.clare.core.scheduler.JobExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;

@Log4j2
class JobContext {
    private List<JobExecutor> jobExecutors = new ArrayList<>();
    @Getter
    @Setter
    private volatile EventJob eventJob;
    @Getter
    private volatile ScheduledFuture scheduledFuture;

    JobContext() {
    }

    public void addConsumer(JobExecutor jobExecutor) {
        synchronized (this.jobExecutors) {
            this.jobExecutors.add(jobExecutor);
        }
    }

    public void execute() {
        if (isCancel()) return;
        for (JobExecutor jobExecutor : jobExecutors) {
            try {
                jobExecutor.execute(eventJob);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public boolean isCancel() {
        return eventJob == null || !eventJob.getEnabled();
    }

    public void setScheduledFuture(ScheduledFuture scheduledFuture) {
        if (this.scheduledFuture != null) {
            try {
                this.scheduledFuture.cancel(false);
            } catch (Exception e) {
                log.warn(e.getMessage());
            }
        }
        this.scheduledFuture = scheduledFuture;
    }
}

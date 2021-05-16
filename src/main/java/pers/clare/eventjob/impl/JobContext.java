package pers.clare.eventjob.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ScheduledFuture;

class JobContext {
    private static final Logger log = LogManager.getLogger();
    private volatile EventJob eventJob;
    private volatile ScheduledFuture<?> scheduledFuture;

    JobContext(EventJob eventJob) {
        this.eventJob = eventJob;
    }

    public void stop() {
        if (this.scheduledFuture == null) return;
        try {
            this.scheduledFuture.cancel(false);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
        this.scheduledFuture = null;
    }

    public boolean isCancel() {
        return eventJob == null || !eventJob.getEnabled();
    }

    public void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
        stop();
        this.scheduledFuture = scheduledFuture;
    }

    public EventJob getEventJob() {
        return eventJob;
    }

    public void setEventJob(EventJob eventJob) {
        this.eventJob = eventJob;
    }
}

package pers.clare.eventjob.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.lang.NonNull;

import java.util.concurrent.ScheduledFuture;

class JobContext {
    private static final Logger log = LogManager.getLogger();
    private volatile EventJob eventJob;
    private volatile ScheduledFuture<?> scheduledFuture;
    private volatile boolean running = false;

    /**
     * waiting for check reply
     */
    private volatile boolean waiting = false;

    JobContext(@NonNull EventJob eventJob) {
        setEventJob(eventJob);
    }

    void stop() {
        ScheduledFuture<?> future;
        synchronized (this) {
            if ((future = this.scheduledFuture) == null) return;
            this.scheduledFuture = null;
        }
        try {
            future.cancel(false);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }

    boolean isCancel() {
        return !eventJob.getEnabled();
    }

    void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
        stop();
        this.scheduledFuture = scheduledFuture;
    }

    public EventJob getEventJob() {
        return eventJob;
    }

    void setEventJob(@NonNull EventJob eventJob) {
        this.eventJob = eventJob;
    }

    boolean isRunning() {
        return running;
    }

    void start() {
        running = true;
    }

    void end() {
        running = false;
    }

    boolean isWaiting() {
        return waiting;
    }

    void pause() {
        waiting = true;
    }

    void proceed() {
        waiting = false;
    }
}

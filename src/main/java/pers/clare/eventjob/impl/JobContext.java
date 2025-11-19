package pers.clare.eventjob.impl;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import pers.clare.eventjob.vo.EventJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.lang.NonNull;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
class JobContext {
    @Getter
    private EventJob eventJob;
    private ScheduledFuture<?> scheduledFuture;
    @Getter
    private volatile long version;
    private volatile boolean running = false;

    void stop() {
        ScheduledFuture<?> future;
        synchronized (this) {
            future = this.scheduledFuture;
            this.scheduledFuture = null;
            if (future == null) return;
        }
        try {
            future.cancel(false);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        } finally {
            log.debug("Old ScheduledFuture stopped.");
        }
    }

    boolean isCancel() {
        return !Boolean.TRUE.equals(eventJob.getEnabled());
    }

    void setScheduledFuture(ScheduledFuture<?> scheduledFuture) {
        stop();
        this.scheduledFuture = scheduledFuture;
    }

    EventJob setEventJob(@NonNull EventJob eventJob) {
        var old = this.eventJob;
        this.eventJob = eventJob;
        this.version = System.currentTimeMillis();
        return old;
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

    public boolean checkVersion(long version) {
        return version == this.version;
    }
}

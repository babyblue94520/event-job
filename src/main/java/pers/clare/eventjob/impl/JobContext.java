package pers.clare.eventjob.impl;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import pers.clare.eventjob.vo.EventJob;
import org.springframework.lang.NonNull;

import java.util.Objects;
import java.util.concurrent.ScheduledFuture;

@Log4j2
@Getter
class JobContext {
    private EventJob eventJob;
    private ScheduledFuture<?> future;
    private volatile String cron;
    private volatile String timezone;
    private volatile boolean running = false;

    public boolean updateCron() {
        if (this.eventJob == null) return false;
        if (Objects.equals(this.cron, this.eventJob.getCron())
            && Objects.equals(this.timezone, this.eventJob.getTimezone())
        ) {
            return false;
        }
        this.cron = this.eventJob.getCron();
        this.timezone = this.eventJob.getTimezone();
        return true;
    }

    void stop() {
        ScheduledFuture<?> temp;
        synchronized (this) {
            this.cron = null;
            this.timezone = null;
            temp = this.future;
            this.future = null;
        }
        if (temp == null) return;

        try {
            temp.cancel(false);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        } finally {
            log.debug("Stopped old task.");
        }
    }

    boolean isCancel() {
        return !eventJob.getEnabled();
    }

    void setFuture(ScheduledFuture<?> future) {
        this.future = future;
    }

    void setEventJob(@NonNull EventJob eventJob) {
        this.eventJob = eventJob;
    }

    void start() {
        running = true;
    }

    void end() {
        running = false;
    }
}

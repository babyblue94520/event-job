package pers.clare.core.scheduler;

import lombok.extern.log4j.Log4j2;
import org.springframework.scheduling.support.CronSequenceGenerator;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Log4j2
class JobContext {
    Scheduler scheduler;
    EventJob eventJob;
    CronSequenceGenerator cronSequenceGenerator;
    ScheduledFuture scheduledFuture;
    List<Consumer<EventJob>> consumers;

    JobContext(
            Scheduler scheduler
            , List<Consumer<EventJob>> consumers
    ) {
        this.scheduler = scheduler;
        this.consumers = consumers;
    }

    public void setEventJob(EventJob eventJob) {
        this.eventJob = eventJob;
        if (eventJob == null) {
            cronSequenceGenerator = null;
        } else {
            cronSequenceGenerator = JobUtil.buildCronGenerator(eventJob.getCron(), eventJob.getTimezone());
        }
        stop();
        start();
    }

    public void addConsumer(Consumer<EventJob> consumer) {
        synchronized (this.consumers) {
            this.consumers.add(consumer);
        }
    }

    public void start() {
        if (eventJob == null || !eventJob.getEnabled()) return;
        log.info("start");
        this.eventJob.setPrevTime(this.eventJob.getNextTime());
        this.eventJob.setNextTime(JobUtil.getNextTime(cronSequenceGenerator));
        this.scheduledFuture = scheduler.getScheduledExecutorService().schedule(this::job, this.eventJob.getNextTime()-System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        log.info("stop");
        if (this.scheduledFuture == null) return;
        this.scheduledFuture.cancel(false);
        this.scheduledFuture = null;

    }

    public void job() {
        log.info("job");
        try {
            this.eventJob.setPrevTime(this.eventJob.getNextTime());
            this.eventJob.setNextTime(JobUtil.getNextTime(cronSequenceGenerator));
            scheduler.getJobStore().executeLock(eventJob.getInstance(), eventJob, this::execute);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void execute() {
        log.info("execute");
        for (Consumer<EventJob> consumer : consumers) {
            try {
                consumer.accept(eventJob);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        start();
    }
}

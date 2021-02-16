package pers.clare.core.scheduler;

import lombok.extern.log4j.Log4j2;
import org.springframework.scheduling.support.CronSequenceGenerator;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Log4j2
class JobContext {
    private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private Scheduler scheduler;
    private EventJob eventJob;
    private CronSequenceGenerator cronSequenceGenerator;
    private ScheduledFuture scheduledFuture;
    private List<Consumer<EventJob>> consumers = new ArrayList<>();

    JobContext(
            Scheduler scheduler
    ) {
        this.scheduler = scheduler;
    }

    public void reload(EventJob eventJob) {
        if (Objects.equals(this.eventJob, eventJob)) return;
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
        this.eventJob.setPrevTime(this.eventJob.getNextTime());
        this.eventJob.setNextTime(JobUtil.getNextTime(cronSequenceGenerator));
        this.scheduledFuture = scheduledExecutorService.schedule(this::job, this.eventJob.getNextTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        log.info("stop");
        if (this.scheduledFuture == null) return;
        this.scheduledFuture.cancel(false);
        this.scheduledFuture = null;

    }

    public void job() {
        try {
            this.eventJob.setPrevTime(this.eventJob.getNextTime());
            this.eventJob.setNextTime(JobUtil.getNextTime(cronSequenceGenerator));
            scheduler.getJobStore().executeLock(eventJob.getInstance(), eventJob, this::execute);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        start();
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
    }
}

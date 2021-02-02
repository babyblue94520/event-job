package pers.clare.core.scheduler;

import org.springframework.scheduling.support.CronSequenceGenerator;
import pers.clare.core.scheduler.bo.EventJob;

import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

class JobContext {
    Scheduler scheduler;
    EventJob eventJob;
    CronSequenceGenerator cronSequenceGenerator;
    ScheduledFuture scheduledFuture;
    List<Consumer<EventJob>> consumers;

    JobContext(Scheduler scheduler, List<Consumer<EventJob>> consumers) {
        this.scheduler = scheduler;
        this.consumers = consumers;
    }

    public void setEventJob(EventJob eventJob) {
        this.eventJob = eventJob;
        if (eventJob == null) {
            cronSequenceGenerator = null;
        } else {
            String cron = eventJob.getCron();
            String timezone = eventJob.getTimezone();
            if (timezone == null) {
                cronSequenceGenerator = new CronSequenceGenerator(cron);
            } else {
                cronSequenceGenerator = new CronSequenceGenerator(cron, TimeZone.getTimeZone(ZoneId.of(timezone)));
            }
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    public void addConsumer(Consumer<EventJob> consumer) {
        this.consumers.add(consumer);
    }


    public void start() {
        long nextTime = cronSequenceGenerator.next(new Date()).getTime();
        this.scheduledFuture = scheduler.scheduledExecutorService.schedule(this::job, nextTime, TimeUnit.MILLISECONDS);
    }

    public void refresh(EventJob eventJob) {

    }

    public void job() {
        execute();
        start();
    }

    public void execute() {
        for (Consumer<EventJob> consumer : consumers) {
            try {
                consumer.accept(eventJob);
            } catch (Exception e) {

            }
        }
    }

    public void stop() {
        this.scheduledFuture.cancel(false);
    }
}

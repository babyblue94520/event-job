package pers.clare.core.scheduler;

import org.springframework.scheduling.support.CronSequenceGenerator;
import pers.clare.core.scheduler.bo.Job;
import pers.clare.core.scheduler.bo.JobKey;
import pers.clare.core.scheduler.bo.ScheduleJob;

import javax.sql.DataSource;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class Scheduler {

    private String instance = "scheduler";

    private DataSource dataSource;

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(3);

    private ConcurrentMap<JobKey, JobContext> jobContexts = new ConcurrentHashMap<>();

    private ConcurrentMap<JobKey, List<Consumer<ScheduleJob>>> jobConsumers = new ConcurrentHashMap<>();

    private void init(){

    }

    public void add(Job job) {
        jobConsumers.get(job);
    }

    void on(JobKey jobKey, Consumer<ScheduleJob> consumer) {
        List<Consumer<ScheduleJob>> consumers = jobConsumers.get(jobKey);
        if (consumers == null) {
            jobConsumers.put(jobKey, consumers = new ArrayList<>());
        }
        consumers.add(consumer);
    }

    class JobContext {
        ScheduleJob scheduleJob;
        CronSequenceGenerator cronSequenceGenerator;
        ScheduledFuture scheduledFuture;
        List<Consumer<ScheduleJob>> consumers;

        JobContext(ScheduleJob scheduleJob, List<Consumer<ScheduleJob>> consumers) {
            this.scheduleJob = scheduleJob;
            this.consumers = consumers;

            String cron = scheduleJob.getCron();
            String timezone = scheduleJob.getTimezone();
            if (timezone == null) {
                this.cronSequenceGenerator = new CronSequenceGenerator(cron);
            } else {
                cronSequenceGenerator = new CronSequenceGenerator(cron, TimeZone.getTimeZone(ZoneId.of(timezone)));
            }
            long nextTime = cronSequenceGenerator.next(new Date()).getTime();
            this.scheduledFuture = scheduledExecutorService.schedule(this::execute, nextTime, TimeUnit.MILLISECONDS);
        }


        void refresh(ScheduleJob scheduleJob) {

        }

        void execute() {
            for (Consumer<ScheduleJob> consumer : consumers) {
                try {
                    consumer.accept(scheduleJob);
                } catch (Exception e) {

                }
            }
            long nextTime = cronSequenceGenerator.next(new Date()).getTime();
            this.scheduledFuture = scheduledExecutorService.schedule(this::execute, nextTime, TimeUnit.MILLISECONDS);
        }

        void stop() {
            this.scheduledFuture.cancel(false);
        }
    }
}

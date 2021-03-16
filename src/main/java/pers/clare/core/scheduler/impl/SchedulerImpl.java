package pers.clare.core.scheduler.impl;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.CommandLineRunner;
import pers.clare.core.scheduler.*;
import pers.clare.core.scheduler.exception.JobException;

import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class SchedulerImpl implements Scheduler, CommandLineRunner {
    protected static final String eventSplit = ",";

    protected static final String defaultInstance = "scheduler";

    static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

    private final ConcurrentMap<String, ConcurrentMap<String, JobContext>> jobGroupContexts = new ConcurrentHashMap<>();

    @Getter
    private final String instance;
    @Getter
    private final String topic;

    @Getter(AccessLevel.PACKAGE)
    private JobStore jobStore;

    private EventJobMessageService eventJobMessageService;

    private long reloadInterval = 60 * 1000;

    private final AtomicInteger executingCount = new AtomicInteger();

    private long nextAllowReloadTime = 0;

    public SchedulerImpl(DataSource dataSource) {
        this(defaultInstance, dataSource, null, null);
    }

    public SchedulerImpl(String instance, DataSource dataSource) {
        this(instance, dataSource, null, null);
    }

    public SchedulerImpl(
            DataSource dataSource
            , String topic
            , EventJobMessageService eventJobMessageService
    ) {
        this(defaultInstance, dataSource, topic, eventJobMessageService);
    }

    public SchedulerImpl(
            String instance
            , DataSource dataSource
            , String topic
            , EventJobMessageService eventJobMessageService
    ) {
        this.instance = instance;
        this.jobStore = new JobStoreImpl(dataSource);
        this.topic = topic;
        this.eventJobMessageService = eventJobMessageService;

        if (this.eventJobMessageService != null) {
            this.eventJobMessageService.onConnected(this::reload);
            this.eventJobMessageService.addListener(topic, this::parse);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        if (eventJobMessageService == null) {
            executor.scheduleAtFixedRate(this::reload, 0, reloadInterval, TimeUnit.MILLISECONDS);
        } else {
            executor.scheduleAtFixedRate(this::reload, 0, reloadInterval * 10, TimeUnit.MILLISECONDS);
        }
    }

    private void parse(String body){
        String[] data = body.split(eventSplit);
        if (data.length != 2) return;
        String group = data[0];
        String name = data[1];
        try {
            reload(group, name);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * register job executor
     */
    public void register(String group, String name, JobExecutor jobExecutor) {
        ConcurrentMap<String, JobContext> jobContext = jobGroupContexts.get(group);
        if (jobContext == null) {
            jobGroupContexts.put(group, jobContext = new ConcurrentHashMap<>());
        }
        JobContext context = jobContext.get(name);
        if (context == null) {
            jobContext.put(name, context = new JobContext());
        }
        context.addConsumer(jobExecutor);
    }

    /**
     * add or update job
     */
    public void add(Job job) throws RuntimeException {
        String group = job.getGroup();
        String name = job.getName();
        Long nextTime = JobUtil.getNextTime(job.getCron(), job.getTimezone());
        EventJob eventJob = jobStore.find(instance, group, name);
        if (eventJob == null) {
            jobStore.insert(instance, job, nextTime);
            reload(group, name);
            notifyChange(group, name);
        } else if (!equals(job, eventJob)) {
            jobStore.update(instance, job, nextTime);
            reload(eventJob);
            notifyChange(group, name);
        }
    }

    /**
     * remove job
     */
    public void remove(String group, String name) throws RuntimeException {
        try {
            jobStore.delete(instance, group, name);
            reload(group, name);
            notifyChange(group, name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * enable job (start)
     */
    @Override
    public void enable(String group, String name) throws RuntimeException {
        jobStore.enable(instance, group, name);
        reload(group, name);
        notifyChange(group, name);
    }

    /**
     * disable job (stop)
     */
    @Override
    public void disable(String group, String name) throws RuntimeException {
        jobStore.disable(instance, group, name);
        reload(group, name);
        notifyChange(group, name);
    }

    public void reload() {
        try {
            long nowTime = System.currentTimeMillis();
            if (nowTime < nextAllowReloadTime) return;
            nextAllowReloadTime = nowTime + reloadInterval;
            List<EventJob> eventJobs = jobStore.findAll(instance);
            for (EventJob eventJob : eventJobs) {
                reload(eventJob);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void reload(String group, String name) throws JobException {
        JobContext jobContext = getJobContext(group, name);
        if (jobContext == null) return;
        jobContext.setEventJob(jobStore.find(instance, group, name));
        schedule(jobContext);
    }

    private void reload(EventJob eventJob) {
        if (eventJob == null) return;
        JobContext jobContext = getJobContext(eventJob.getGroup(), eventJob.getName());
        if (jobContext == null) return;
        jobContext.setEventJob(eventJob);
        schedule(jobContext);
    }

    /**
     * add job to schedule
     */
    private void schedule(JobContext jobContext) {
        if (jobContext.isCancel()) return;
        jobContext.setScheduledFuture(executor.schedule(() -> {
            if (jobContext.isCancel()) return;
            delay();
            executingCount.getAndIncrement();
            try {
                if(jobContext.isCancel())return;
                EventJob eventJob = jobContext.getEventJob();
                Long nextTime = JobUtil.getNextTime(eventJob.getCron(), eventJob.getTimezone());
                eventJob.setPrevTime(eventJob.getNextTime());
                eventJob.setNextTime(nextTime);
                eventJob.setStartTime(System.currentTimeMillis());
                eventJob.setEndTime(0L);
                int count = jobStore.executor(jobContext.getEventJob(), jobContext::execute);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            executingCount.getAndDecrement();
            schedule(jobContext);
        }, jobContext.getEventJob().getNextTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS));
    }

    private JobContext getJobContext(String group, String name) {
        ConcurrentMap<String, JobContext> jobContexts = jobGroupContexts.get(group);
        if (jobContexts == null) return null;
        JobContext context = jobContexts.get(name);
        return context;
    }

    private boolean equals(Job job, EventJob eventJob) {
        if (job == null && eventJob == null) return true;
        if (job == null || eventJob == null) return false;
        return Objects.equals(job.getGroup(), eventJob.getGroup())
                && Objects.equals(job.getName(), eventJob.getName())
                && Objects.equals(job.getDescription(), eventJob.getDescription())
                && Objects.equals(job.getTimezone(), eventJob.getTimezone())
                && Objects.equals(job.getCron(), eventJob.getCron())
                && Objects.equals(job.getEnabled(), eventJob.getEnabled())
                && Objects.equals(job.getData(), eventJob.getData());
    }

    private void notifyChange(String group, String name) {
        if (eventJobMessageService == null) return;
        eventJobMessageService.send(topic, group + eventSplit + name);
    }

    /**
     * 根據當前執行中的任務數量和 CPU 使用率，延遲執行任務
     */
    private void delay() {
        long delay = (long) (executingCount.get() * 10 + (getCpuUsage() * 100));
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private double getCpuUsage() {
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory
                .getOperatingSystemMXBean()).getSystemCpuLoad();
    }

}


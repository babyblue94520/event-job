package pers.clare.eventjob.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.CommandLineRunner;
import pers.clare.eventjob.*;
import pers.clare.eventjob.exception.JobException;
import pers.clare.eventjob.exception.JobNotExistException;
import pers.clare.eventjob.function.JobExecutor;
import pers.clare.eventjob.util.JobUtil;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SchedulerImpl implements Scheduler, InitializingBean, DisposableBean, CommandLineRunner {
    private static final Logger log = LogManager.getLogger();

    protected static final String eventSplit = ",";

    private final ConcurrentMap<String, ConcurrentMap<String, JobContext>> jobGroupContexts = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, List<JobExecutor>> eventJobExecutors = new ConcurrentHashMap<>();

    private final JobStore jobStore;

    private final EventJobMessageService eventJobMessageService;

    private final AtomicInteger executingCount = new AtomicInteger();

    private ScheduledExecutorService executor;

    private boolean ready = false;

    private long nextAllowReloadTime = 0;

    private String instance = "eventJobScheduler";

    private String topic = "event.job";

    private long reloadInterval = 60 * 1000;

    private int threadCount = Runtime.getRuntime().availableProcessors() * 2;

    // SECONDS
    private int shutdownWait = 180;


    private String changeTopic;
    private String executeTopic;

    public SchedulerImpl(JobStore jobStore) {
        this.jobStore = jobStore;
        this.eventJobMessageService = null;
    }

    public SchedulerImpl(JobStore jobStore, EventJobMessageService eventJobMessageService) {
        this.jobStore = jobStore;
        this.eventJobMessageService = eventJobMessageService;

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.eventJobMessageService != null) {
            this.changeTopic = this.topic + '.' + instance + ".change";
            this.executeTopic = this.topic + '.' + instance + ".execute";
            this.eventJobMessageService.onConnected(this::reload);
            this.eventJobMessageService.addListener(changeTopic, this::reloadEventHandler);
            this.eventJobMessageService.addListener(executeTopic, this::executeEventHandler);
        }

    }

    @Override
    public void destroy() throws Exception {
        this.executor.shutdown();
        if (this.executor.awaitTermination(shutdownWait, TimeUnit.SECONDS)) {
            log.info("Shutdown completed");
        } else {
            log.warn("Shutdown interrupted");
        }
    }

    @Override
    public void run(String... args) {
        this.executor = Executors.newScheduledThreadPool(threadCount);
        this.ready = true;
        if (this.eventJobMessageService == null) {
            this.executor.scheduleAtFixedRate(this::reload, 0, reloadInterval, TimeUnit.MILLISECONDS);
        } else {
            this.executor.scheduleAtFixedRate(this::reload, 0, reloadInterval * 10, TimeUnit.MILLISECONDS);
        }
    }

    private void reloadEventHandler(String body) {
        try {
            String[] data = body.split(eventSplit);
            if (data.length == 1) {
                reload(data[0]);
            } else if (data.length == 2) {
                reload(data[0], data[1]);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void executeEventHandler(String body) {
        try {
            String[] data = body.split(eventSplit);
            if (data.length == 1) {
                doExecute(data[0]);
            } else if (data.length == 2) {
                doExecute(data[0], data[1]);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * register job executor
     */
    public JobExecutor addExecutor(String event, JobExecutor jobExecutor) {
        List<JobExecutor> jobExecutors = eventJobExecutors.computeIfAbsent(event, (key) -> new CopyOnWriteArrayList<>());
        jobExecutors.add(jobExecutor);
        return jobExecutor;
    }

    public void removeExecutor(String event, JobExecutor jobExecutor) {
        List<JobExecutor> jobExecutors = eventJobExecutors.get(event);
        if (jobExecutors == null) return;
        jobExecutors.removeAll(Collections.singletonList(jobExecutor));
    }

    /**
     * add or update job
     */
    public void add(Job job) {
        String group = job.getGroup();
        String name = job.getName();
        long nextTime = JobUtil.getNextTime(job.getCron(), job.getTimezone());
        EventJob eventJob = jobStore.find(instance, group, name);
        if (eventJob == null) {
            jobStore.insert(instance, job, nextTime);
            reload(group, name);
            notifyChange(group, name);
        } else if (!hasChange(job, eventJob)) {
            jobStore.update(instance, job, nextTime);
            reload(group, name);
            notifyChange(group, name);
        }
    }

    /**
     * remove job
     */
    public void remove(String group) {
        try {
            jobStore.delete(instance, group);
            reload(group);
            notifyChange(group);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void remove(String group, String name) {
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
    public void enable(String group) {
        jobStore.enable(instance, group);
        reload(group);
        notifyChange(group);
    }

    @Override
    public void enable(String group, String name) {
        jobStore.enable(instance, group, name);
        reload(group, name);
        notifyChange(group, name);
    }

    /**
     * disable job (stop)
     */
    @Override
    public void disable(String group) {
        jobStore.disable(instance, group);
        reload(group);
        notifyChange(group);
    }

    @Override
    public void disable(String group, String name) {
        jobStore.disable(instance, group, name);
        reload(group, name);
        notifyChange(group, name);
    }

    @Override
    public void execute(String group) {
        if (this.eventJobMessageService == null) {
            doExecute(group);
        } else {
            notifyExecute(group);
        }
    }

    @Override
    public void execute(String group, String name) {
        if (this.eventJobMessageService == null) {
            doExecute(group, name);
        } else {
            notifyExecute(group, name);
        }
    }

    public void reload() {
        if (!ready) return;
        long nowTime = System.currentTimeMillis();
        if (nowTime < nextAllowReloadTime) return;
        nextAllowReloadTime = nowTime + reloadInterval;
        try {
            List<EventJob> eventJobs = jobStore.findAll(instance);
            for (EventJob eventJob : eventJobs) {
                reload(eventJob);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void reload(String group) throws JobException {
        List<EventJob> eventJobs = jobStore.findAll(instance, group);
        for (EventJob eventJob : eventJobs) {
            reload(eventJob);
        }
    }

    public void reload(String group, String name) throws JobException {
        EventJob eventJob = jobStore.find(instance, group, name);
        if (eventJob == null) {
            JobContext jobContext = removeJobContext(group, name);
            if (jobContext == null) return;
            jobContext.stop();
        } else {
            reload(eventJob);
        }
    }

    private void reload(EventJob eventJob) {
        if (!ready) return;
        if (eventJob == null) return;
        JobContext jobContext = getJobContext(eventJob);
        jobContext.setEventJob(eventJob);
        schedule(jobContext);
    }

    private boolean isCancel(JobContext jobContext) {
        return jobContext == null || executor == null || jobContext.isCancel() || !eventJobExecutors.containsKey(jobContext.getEventJob().getEvent());
    }

    /**
     * add job to schedule
     */
    private void schedule(JobContext jobContext) {
        if (isCancel(jobContext)) return;
        jobContext.setScheduledFuture(executor.schedule(() -> {
            EventJob eventJob = jobContext.getEventJob();
            if (isCancel(jobContext)) return;
            delay();
            executingCount.getAndIncrement();
            boolean stop = false;
            try {
                Long nextTime = JobUtil.getNextTime(eventJob.getCron(), eventJob.getTimezone());
                eventJob.setPrevTime(eventJob.getNextTime());
                eventJob.setNextTime(nextTime);
                eventJob.setStartTime(System.currentTimeMillis());
                eventJob.setEndTime(0L);
                jobStore.executor(jobContext.getEventJob(), this::doExecute);
            } catch (JobNotExistException e) {
                log.error(e.getMessage(), e);
                stop = true;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                executingCount.getAndDecrement();
                if (stop) {
                    reload(eventJob.getGroup(), eventJob.getName());
                } else {
                    schedule(jobContext);
                }
            }
        }, jobContext.getEventJob().getNextTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS));
    }

    private void doExecute(String group) {
        List<EventJob> eventJobs = jobStore.findAll(instance, group);
        for (EventJob eventJob : eventJobs) {
            doExecute(eventJob);
        }
    }

    private void doExecute(String group, String name) {
        doExecute(jobStore.find(instance, group, name));
    }

    private void doExecute(EventJob eventJob) {
        if (eventJob == null) return;
        List<JobExecutor> jobExecutors = eventJobExecutors.get(eventJob.getEvent());
        if (jobExecutors == null) return;
        for (JobExecutor jobExecutor : jobExecutors) {
            jobExecutor.execute(eventJob);
        }
    }

    private JobContext getJobContext(EventJob eventJob) {
        return jobGroupContexts.computeIfAbsent(eventJob.getGroup(), (key) -> new ConcurrentHashMap<>())
                .computeIfAbsent(eventJob.getName(), (key) -> new JobContext(eventJob));
    }

    private JobContext removeJobContext(String group, String name) {
        ConcurrentMap<String, JobContext> groupContexts = jobGroupContexts.get(group);
        if (groupContexts == null) return null;
        return groupContexts.remove(name);
    }

    private boolean hasChange(Job job, EventJob eventJob) {
        if (job == null && eventJob == null) return true;
        if (job == null || eventJob == null) return false;
        return Objects.equals(job.getGroup(), eventJob.getGroup())
                && Objects.equals(job.getName(), eventJob.getName())
                && Objects.equals(job.getEvent(), eventJob.getEvent())
                && Objects.equals(job.getDescription(), eventJob.getDescription())
                && Objects.equals(job.getTimezone(), eventJob.getTimezone())
                && Objects.equals(job.getCron(), eventJob.getCron())
                && Objects.equals(job.getEnabled(), eventJob.getEnabled())
                && Objects.equals(job.getData(), eventJob.getData());
    }

    private void notifyChange(String group) {
        if (eventJobMessageService == null) return;
        eventJobMessageService.send(changeTopic, group);
    }

    private void notifyChange(String group, String name) {
        if (eventJobMessageService == null) return;
        eventJobMessageService.send(changeTopic, group + eventSplit + name);
    }

    private void notifyExecute(String group) {
        if (eventJobMessageService == null) return;
        eventJobMessageService.send(executeTopic, group);
    }

    private void notifyExecute(String group, String name) {
        if (eventJobMessageService == null) return;
        eventJobMessageService.send(executeTopic, group + eventSplit + name);
    }

    /**
     * 根據當前執行中的任務數量和 CPU 使用率，延遲執行任務
     */
    private void delay() {
        long delay = (long) (executingCount.get() * 10L + (getCpuUsage() * 100));
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

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setReloadInterval(long reloadInterval) {
        this.reloadInterval = reloadInterval;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public void setShutdownWait(int shutdownWait) {
        this.shutdownWait = shutdownWait;
    }
}


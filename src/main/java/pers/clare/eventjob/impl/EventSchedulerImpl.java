package pers.clare.eventjob.impl;

import com.sun.management.OperatingSystemMXBean;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.CommandLineRunner;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.StringUtils;
import pers.clare.eventjob.*;
import pers.clare.eventjob.constant.EventJobEventType;
import pers.clare.eventjob.constant.EventJobStatus;
import pers.clare.eventjob.exception.JobException;
import pers.clare.eventjob.function.JobHandler;
import pers.clare.eventjob.util.JobUtil;
import pers.clare.eventjob.vo.EventJob;
import pers.clare.eventjob.vo.EventJobKey;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Log4j2
@SuppressWarnings("unused")
public class EventSchedulerImpl implements EventScheduler, InitializingBean, DisposableBean, CommandLineRunner {
    protected static final String EVENT_SPLIT = "\n";

    protected static final Pattern eventSplitPattern = Pattern.compile("([^" + EVENT_SPLIT + "]+)" + EVENT_SPLIT + "?");

    private final ConcurrentMap<EventJobKey, JobContext> jobContextMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, List<JobHandler>> eventJobHandlersMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<EventJobKey, Map<EventJobKey, EventJobKey>> afterEventJobsMap = new ConcurrentHashMap<>();

    private final AtomicInteger executingCount = new AtomicInteger();

    private final EventJobProperties properties;

    private final JobStore jobStore;

    private final EventJobMessageService eventJobMessageService;

    private ScheduledExecutorService executor;

    private boolean ready = false;

    private long nextAllowReloadTime = 0;

    private volatile boolean destroyed = false;

    public EventSchedulerImpl(@NonNull EventJobProperties properties, @NonNull JobStore jobStore) {
        this(properties, jobStore, null);
    }

    public EventSchedulerImpl(@NonNull EventJobProperties properties, @NonNull JobStore jobStore, EventJobMessageService eventJobMessageService) {
        this.properties = properties;
        this.jobStore = jobStore;
        this.eventJobMessageService = eventJobMessageService;
    }

    @Override
    public void afterPropertiesSet() {
        if (eventJobMessageService != null) {
            eventJobMessageService.addListener(this::eventHandler);
        }
    }

    @Override
    public void destroy() {
        destroyed = true;
        if (executor == null) return;
        log.info("Shutdown...");
        executor.shutdownNow();
        log.info("Shutdown completed");
    }

    @Override
    public void run(String... args) {
        executor = Executors.newScheduledThreadPool(properties.getThreadCount(), new CustomizableThreadFactory("event-job-"));
        executor.scheduleAtFixedRate(this::reload, 0, properties.getReloadInterval().toMillis(), TimeUnit.MILLISECONDS);
        executor.scheduleAtFixedRate(this::updateActiveTime, properties.getUpdateActiveInterval().toMillis(), properties.getUpdateActiveInterval().toMillis(), TimeUnit.MILLISECONDS);
        ready = true;
    }

    /**
     * register job executor
     */
    public JobHandler addHandler(String event, JobHandler jobHandler) {
        List<JobHandler> jobHandlers = eventJobHandlersMap.computeIfAbsent(event, key -> new CopyOnWriteArrayList<>());
        jobHandlers.add(jobHandler);
        return jobHandler;
    }

    public void removeHandler(String event, JobHandler jobHandler) {
        List<JobHandler> jobHandlers = eventJobHandlersMap.get(event);
        if (jobHandlers == null) return;
        jobHandlers.remove(jobHandler);
    }

    @Override
    public String getInstance() {
        return properties.getInstance();
    }

    @Override
    public List<EventJob> findAll() {
        return jobStore.findAll(getInstance());
    }

    @Override
    public List<EventJob> findAll(String group) {
        return jobStore.findAll(getInstance(), group);
    }

    @Override
    public EventJob find(String group, String name) {
        return jobStore.find(getInstance(), group, name);
    }

    public void add(EventJob job) {
        if (job == null) return;
        String group = job.getGroup();
        String name = job.getName();
        long nextTime = 0L;
        if (StringUtils.hasLength(job.getCron())) {
            nextTime = JobUtil.getNextTime(job.getCron(), job.getTimezone());
        }
        EventJob eventJob = jobStore.find(getInstance(), group, name);
        if (eventJob == null) {
            jobStore.insert(getInstance(), job, nextTime);
        } else if (equals(job, eventJob)) {
            jobStore.update(getInstance(), job, nextTime);
        } else {
            return;
        }
        eventJob = jobStore.find(getInstance(), group, name);
        reload(eventJob);
        notifyChange(group, name);
    }

    /**
     * remove job
     */
    public void remove(String group) {
        try {
            jobStore.delete(getInstance(), group);
            reload(group);
            notifyChange(group);
        } catch (Exception e) {
            throw new JobException(e);
        }
    }

    public void remove(String group, String name) {
        try {
            jobStore.delete(getInstance(), group, name);
            reload(group, name);
            notifyChange(group, name);
        } catch (Exception e) {
            throw new JobException(e);
        }
    }

    @Override
    public void enable(String group) {
        jobStore.enable(getInstance(), group);
        reload(group);
        notifyChange(group);
    }

    @Override
    public void enable(String group, String name) {
        jobStore.enable(getInstance(), group, name);
        reload(group, name);
        notifyChange(group, name);
    }

    /**
     * disable job (stop)
     */
    @Override
    public void disable(String group) {
        jobStore.disable(getInstance(), group);
        reload(group);
        notifyChange(group);
    }

    @Override
    public void disable(String group, String name) {
        jobStore.disable(getInstance(), group, name);
        reload(group, name);
        notifyChange(group, name);
    }

    @Override
    public void execute(String group) {
        if (eventJobMessageService == null) {
            executeJobHandler(group, System.currentTimeMillis());
        } else {
            notifyExecute(group);
        }
    }

    @Override
    public void execute(String group, String name) {
        if (eventJobMessageService == null) {
            executeJobHandler(group, name, System.currentTimeMillis());
        } else {
            notifyExecute(group, name);
        }
    }

    private void executeAfterJob(EventJobKey key) {
        if (eventJobMessageService == null) {
            completeJobHandler(key);
        } else {
            notifyComplete(key);
        }
    }

    private void clearNotExists(List<EventJob> eventJobs) {
        Set<EventJobKey> exists = new HashSet<>(eventJobs);
        for (EventJobKey eventJobKey : jobContextMap.keySet()) {
            if (!exists.contains(eventJobKey)) {
                clear(eventJobKey);
            }
        }
    }

    private void clear(EventJobKey eventJobKey) {
        JobContext jobContext = jobContextMap.remove(eventJobKey);
        if (jobContext != null) {
            jobContext.stop();
        }
        for (Map<EventJobKey, EventJobKey> value : afterEventJobsMap.values()) {
            value.remove(eventJobKey);
        }
    }

    private void reload() {
        if (!ready) return;
        long nowTime = System.currentTimeMillis();
        if (nowTime < nextAllowReloadTime) return;
        nextAllowReloadTime = nowTime + properties.getReloadInterval().toMillis();
        try {
            List<EventJob> eventJobs = jobStore.findAll(getInstance());
            clearNotExists(eventJobs);
            for (EventJob eventJob : eventJobs) {
                reload(eventJob);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void reload(String group) throws JobException {
        List<EventJob> eventJobs = jobStore.findAll(getInstance(), group);
        for (EventJob eventJob : eventJobs) {
            reload(eventJob);
        }
    }

    private void reload(String group, String name) throws JobException {
        EventJob eventJob = jobStore.find(getInstance(), group, name);
        if (eventJob == null) {
            clear(new EventJobKey(group, name));
        } else {
            reload(eventJob);
        }
    }

    private void reload(EventJob eventJob) {
        if (!ready) return;
        var jobContext = jobContextMap
                .computeIfAbsent(eventJob, key -> new JobContext());

        jobContext.setEventJob(eventJob);

        if (StringUtils.hasLength(eventJob.getCron()) && eventJob.getEnabled()) {
            jobContext.nextVersion();
            addSchedule(jobContext);
        } else {
            jobContext.stop();
        }
        String afterGroup = eventJob.getAfterGroup();
        String afterName = eventJob.getAfterName();

        if (afterGroup.isEmpty() || afterName.isEmpty()) return;
        afterEventJobsMap
                .computeIfAbsent(new EventJobKey(afterGroup, afterName), key -> new ConcurrentHashMap<>())
                .put(eventJob, eventJob)
        ;
    }

    private void updateActiveTime() {
        long now = System.currentTimeMillis();
        for (JobContext jobContext : jobContextMap.values()) {
            if (!jobContext.isRunning()) continue;
            var eventJob = jobContext.getEventJob();
            jobStore.updateActive(getInstance(), eventJob, now);
        }
    }

    /**
     * add job to schedule
     */
    private void addSchedule(JobContext jobContext) {
        if (executor == null || executor.isShutdown() || executor.isTerminated()) return;
        if (jobContext.isCancel()) return;
        EventJob eventJob = jobContext.getEventJob();
        long version = jobContext.getVersion();
        long delay = JobUtil.getNextDelay(eventJob.getCron(), eventJob.getTimezone());
        ScheduledFuture<?> scheduledFuture = executor.schedule(() -> {
            if (destroyed) return;
            JobContext context = jobContextMap.get(eventJob);
            if (context == null) return;
            if (context.isCancel()) return;
            if (!context.checkVersion(version)) return;
            if (doExecute(context)) {
                addSchedule(context);
            }
        }, delay, TimeUnit.MILLISECONDS);
        jobContext.setScheduledFuture(scheduledFuture);
    }

    private boolean doExecute(JobContext jobContext) {
        return this.doExecute(jobContext, null);
    }

    /**
     * @param executeTime Execution command time. schedule job is null.
     */
    private boolean doExecute(JobContext jobContext, Long executeTime) {
        EventJob eventJob = jobContext.getEventJob();
        List<JobHandler> jobHandlers = getJobHandlers(eventJob.getEvent());
        if (jobHandlers.isEmpty()) return true;
        if (jobContext.isRunning()) return true;

        delayExecute();

        executingCount.getAndIncrement();
        boolean executed = false;
        try {
            String instance = getInstance();
            String group = eventJob.getGroup();
            String name = eventJob.getName();

            JobStatus jobStatus = jobStore.getStatus(instance, group, name);
            if (jobStatus == null) return false;

            int compete;
            if (executeTime == null) {
                long startTime = System.currentTimeMillis();
                long nextTime = getNextTime(eventJob);
                if (Objects.equals(EventJobStatus.EXECUTING, jobStatus.getStatus())) {
                    var activeInterval = properties.getUpdateActiveInterval().toMillis();
                    var checkTime = jobStatus.getLastActiveTime() + (activeInterval * 1.5);
                    if (startTime < checkTime) return true;
                    int count = jobStore.release(instance, group, name, nextTime);
                    if (count == 0) return true;
                }
                compete = jobStore.compete(instance, group, name, nextTime, startTime);
            } else {
                compete = jobStore.compete(instance, group, name, executeTime);
            }
            if (compete == 0) return true;

            jobContext.start();
            List<JobHandler> removes = new ArrayList<>();
            for (JobHandler jobHandler : jobHandlers) {
                try {
                    jobHandler.execute(eventJob);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    if (Boolean.TRUE.equals(properties.getAbortOnError())) {
                        removes.add(jobHandler);
                    }
                }
            }
            if (!removes.isEmpty()) {
                jobHandlers.removeAll(removes);
            }
            executed = true;
            jobStore.finish(instance, group, name, System.currentTimeMillis());
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            jobContext.end();
            executingCount.getAndDecrement();
            if (executed) executeAfterJob(eventJob);
        }
        return false;
    }

    private long getNextTime(EventJob eventJob) {
        if (StringUtils.hasLength(eventJob.getCron())) {
            return JobUtil.getNextTime(eventJob.getCron(), eventJob.getTimezone());
        } else if (StringUtils.hasLength(eventJob.getAfterGroup())) {
            JobContext jobContext = jobContextMap.get(new EventJobKey(eventJob.getAfterGroup(), eventJob.getAfterName()));
            if (jobContext == null) return 0L;
            return getNextTime(jobContext.getEventJob());
        }
        return 0L;
    }

    @NonNull
    private List<JobHandler> getJobHandlers(String event) {
        return eventJobHandlersMap.computeIfAbsent(event, key -> new CopyOnWriteArrayList<>());
    }

    private void executeJobHandler(String group, Long time) {
        for (Map.Entry<EventJobKey, JobContext> entry : jobContextMap.entrySet()) {
            if (Objects.equals(entry.getKey().getGroup(), group)) {
                doExecute(entry.getValue(), time);
            }
        }
    }

    private void executeJobHandler(String group, String name, Long time) {
        executeJobHandler(new EventJobKey(group, name), time);
    }

    private void executeJobHandler(EventJobKey key, Long time) {
        JobContext jobContext = jobContextMap.get(key);
        if (jobContext == null) return;
        doExecute(jobContext, time);
    }

    private void completeJobHandler(EventJobKey key) {
        Map<EventJobKey, EventJobKey> map = afterEventJobsMap.getOrDefault(key, Collections.emptyMap());
        for (EventJobKey value : map.values()) {
            JobContext jobContext = jobContextMap.get(value);
            if (jobContext == null) {
                continue;
            }
            doExecute(jobContext);
        }
    }


    private void notifyChange(String group) {
        notifyEvent(EventJobEventType.CHANGE, group);
    }

    private void notifyChange(String group, String name) {
        notifyEvent(EventJobEventType.CHANGE, group, name);
    }

    private void eventHandler(String body) {
        String[] result = body.split(EVENT_SPLIT);
        String[] array = new String[4];
        System.arraycopy(result, 0, array, 0, result.length);
        String type = array[0];
        switch (type) {
            case EventJobEventType.CHANGE:
                changeEventHandler(array[1], array[2]);
                break;
            case EventJobEventType.EXECUTE:
                executeEventHandler(array[1], array[2], array[3]);
                break;
            case EventJobEventType.COMPLETE:
                completeEventHandler(array[1], array[2]);
                break;
            default:
        }
    }

    private void changeEventHandler(String group, String name) {
        try {
            if (name == null) {
                reload(group);
            } else {
                reload(group, name);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void notifyExecute(String group) {
        notifyEvent(EventJobEventType.EXECUTE, group, "", String.valueOf(System.currentTimeMillis()));
    }

    private void notifyExecute(String group, String name) {
        notifyEvent(EventJobEventType.EXECUTE, group, name, String.valueOf(System.currentTimeMillis()));
    }

    private void executeEventHandler(String group, String name, String time) {
        try {
            if (Objects.equals(name, "")) {
                executeJobHandler(group, Long.valueOf(time));
            } else {
                executeJobHandler(group, name, Long.valueOf(time));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void notifyComplete(EventJobKey key) {
        notifyEvent(EventJobEventType.COMPLETE, key.getGroup(), key.getName());
    }

    private void completeEventHandler(String group, String name) {
        try {
            completeJobHandler(new EventJobKey(group, name));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Calculate the delay time based on the CPU usage rate and the number of currently executed tasks
     */
    private void delayExecute() {
        int count = executingCount.get();
        if (count == 0) return;
        long delay = (long) (count * 10L + (getCpuUsage() * 100));
        if (delay > 100) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private double getCpuUsage() {
        return ((OperatingSystemMXBean) ManagementFactory
                .getOperatingSystemMXBean()).getSystemCpuLoad();
    }

    private void notifyEvent(String type, String... args) {
        if (eventJobMessageService == null) return;
        StringBuilder message = new StringBuilder(type);
        for (String arg : args) {
            message.append(EVENT_SPLIT).append(arg);
        }
        eventJobMessageService.send(message.toString());
    }

    private String[] splitMessage(String message) {
        List<String> list = new ArrayList<>();
        Matcher m = eventSplitPattern.matcher(message);
        while (m.find()) {
            list.add(m.group(1));
        }
        return list.toArray(new String[4]);
    }

    private boolean equals(EventJob source, EventJob target) {
        return Objects.equals(source.getGroup(), target.getGroup())
               && Objects.equals(source.getName(), target.getName())
               && Objects.equals(source.getEvent(), target.getEvent())
               && Objects.equals(source.getDescription(), target.getDescription())
               && Objects.equals(source.getTimezone(), target.getTimezone())
               && Objects.equals(source.getCron(), target.getCron())
               && Objects.equals(source.getEnabled(), target.getEnabled())
               && Objects.equals(source.getAfterGroup(), target.getAfterGroup())
               && Objects.equals(source.getAfterName(), target.getAfterName())
               && Objects.equals(source.getData(), target.getData());
    }
}


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
import pers.clare.eventjob.vo.DependentJob;
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
        ready = true;
        executor.scheduleAtFixedRate(this::reload, 0, properties.getReloadInterval().toMillis(), TimeUnit.MILLISECONDS);
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
        jobHandlers.removeAll(Collections.singletonList(jobHandler));
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
            throw new RuntimeException(e);
        }
    }

    public void remove(String group, String name) {
        try {
            jobStore.delete(getInstance(), group, name);
            reload(group, name);
            notifyChange(group, name);
        } catch (Exception e) {
            throw new RuntimeException(e);
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


    private void reload() {
        if (!ready) return;
        long nowTime = System.currentTimeMillis();
        if (nowTime < nextAllowReloadTime) return;
        nextAllowReloadTime = nowTime + properties.getReloadInterval().toMillis();
        try {
            List<EventJob> eventJobs = jobStore.findAll(getInstance());
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
            JobContext jobContext = jobContextMap.remove(new EventJobKey(group, name));
            if (jobContext == null) return;
            jobContext.stop();
        } else {
            reload(eventJob);
        }
    }

    private void reload(EventJob eventJob) {
        if (!ready) return;
        var jobContext = jobContextMap
                .computeIfAbsent(eventJob, key -> new JobContext());

        var old = jobContext.setEventJob(eventJob);
        if (old != null && Objects.equals(old.getCron(), eventJob.getCron())) {
            return;
        }

        if (StringUtils.hasLength(eventJob.getCron())) {
            addSchedule(jobContext);
        } else {
            jobContext.stop();
        }
        afterEventJobsMap
                .computeIfAbsent(new EventJobKey(eventJob.getAfterGroup(), eventJob.getAfterName()), key -> new ConcurrentHashMap<>())
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
            JobContext context = jobContextMap.get(eventJob);
            if (context == null) return;
            var next = doExecute(context) && context.checkVersion(version);
            if (next) {
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
        if (destroyed) return false;
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
                if (jobContext.isCancel()) return false;
                long startTime = System.currentTimeMillis();
                long nextTime = getOrFindNextTime(eventJob);
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
            for (JobHandler jobHandler : jobHandlers) {
                try {
                    jobHandler.execute(eventJob);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
            jobContext.end();
            executed = true;
            jobStore.finish(instance, group, name, System.currentTimeMillis());
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            executingCount.getAndDecrement();
            reload(jobContext.getEventJob());
            if (executed) executeAfterJob(eventJob);
        }
        return false;
    }

    private long getOrFindNextTime(EventJob eventJob) {
        if (StringUtils.hasLength(eventJob.getCron())) {
            return JobUtil.getNextTime(eventJob.getCron(), eventJob.getTimezone());
        } else if (StringUtils.hasLength(eventJob.getAfterGroup())) {
            return getOrFindNextTime(jobStore.findDependentJob(getInstance(), eventJob.getAfterGroup(), eventJob.getAfterName()));
        }
        return 0L;
    }

    private long getOrFindNextTime(DependentJob dependentJob) {
        if (dependentJob != null) {
            if (StringUtils.hasLength(dependentJob.getCron())) {
                return JobUtil.getNextTime(dependentJob.getCron(), dependentJob.getTimezone());
            } else if (StringUtils.hasLength(dependentJob.getAfterGroup())) {
                return getOrFindNextTime(jobStore.findDependentJob(getInstance(), dependentJob.getAfterGroup(), dependentJob.getAfterName()));
            }
        }
        return 0L;
    }

    @NonNull
    private List<JobHandler> getJobHandlers(String event) {
        return eventJobHandlersMap.computeIfAbsent(event, key -> Collections.emptyList());
    }

    private void executeJobHandler(String group, Long time) {
        if (eventJobHandlersMap.isEmpty()) return;
        List<EventJob> eventJobs = jobStore.findAll(getInstance(), group);
        for (EventJob eventJob : eventJobs) {
            executeJobHandler(eventJob, time);
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
        for (Iterator<Map.Entry<EventJobKey, EventJobKey>> it = map.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<EventJobKey, EventJobKey> entry = it.next();
            JobContext jobContext = jobContextMap.get(entry.getKey());
            if (jobContext == null
                || StringUtils.hasLength(jobContext.getEventJob().getCron())
            ) {
                it.remove();
                continue;
            }
            doExecute(jobContext);
        }
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
}


package pers.clare.eventjob.impl;

import pers.clare.eventjob.vo.DependentJob;
import pers.clare.eventjob.vo.EventJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class EventSchedulerImpl implements EventScheduler, InitializingBean, DisposableBean, CommandLineRunner {
    protected static final String eventSplit = "\n";

    protected static final Pattern eventSplitPattern = Pattern.compile("([^" + eventSplit + "]+)" + eventSplit + "?");

    private static final Logger log = LogManager.getLogger();
    private final ConcurrentMap<String, ConcurrentMap<String, JobContext>> jobGroupContextMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, List<JobHandler>> eventJobHandlersMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Map<String, Map<String, String>>> afterEventJobsMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, String> checkMap = new ConcurrentHashMap<>();

    private final AtomicInteger executingCount = new AtomicInteger();

    private final EventJobProperties eventJobProperties;

    private final JobStore jobStore;

    private final EventJobMessageService eventJobMessageService;


    private ScheduledExecutorService executor;

    private boolean ready = false;

    private long nextAllowReloadTime = 0;

    public EventSchedulerImpl(@NonNull EventJobProperties eventJobProperties, @NonNull JobStore jobStore) {
        this(eventJobProperties, jobStore, null);
    }

    public EventSchedulerImpl(@NonNull EventJobProperties eventJobProperties, @NonNull JobStore jobStore, EventJobMessageService eventJobMessageService) {
        this.eventJobProperties = eventJobProperties;
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
    public void destroy() throws Exception {
        if (executor == null) return;
        log.info("Shutdown...");
        executor.shutdownNow();
        log.info("Shutdown completed");
    }

    @Override
    public void run(String... args) {
        executor = Executors.newScheduledThreadPool(eventJobProperties.getThreadCount(), new CustomizableThreadFactory("event-job-"));
        ready = true;
        executor.scheduleAtFixedRate(this::reload, 0, eventJobProperties.getReloadInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * register job executor
     */
    public JobHandler addHandler(String event, JobHandler jobHandler) {
        List<JobHandler> jobHandlers = eventJobHandlersMap.computeIfAbsent(event, (key) -> new CopyOnWriteArrayList<>());
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
        return eventJobProperties.getInstance();
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

    public void add(@NonNull EventJob job) {
        String group = job.getGroup();
        String name = job.getName();
        Long nextTime = 0L;
        if (StringUtils.hasLength(job.getCron())) {
            nextTime = JobUtil.getNextTime(job.getCron(), job.getTimezone());
        }
        EventJob eventJob = jobStore.find(getInstance(), group, name);
        if (eventJob == null) {
            jobStore.insert(getInstance(), job, nextTime);
            reload(group, name);
            notifyChange(group, name);
        } else if (!hasChange(job, eventJob)) {
            jobStore.update(getInstance(), job, nextTime);
            reload(group, name);
            notifyChange(group, name);
        }
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
            executeJobHandler(group);
        } else {
            notifyExecute(group);
        }
    }

    @Override
    public void execute(String group, String name) {
        if (eventJobMessageService == null) {
            executeJobHandler(group, name);
        } else {
            notifyExecute(group, name);
        }
    }

    private void executeAfterJob(String group, String name) {
        if (eventJobMessageService == null) {
            completeJobHandler(group, name);
        } else {
            notifyComplete(group, name);
        }
    }

    private void reload() {
        if (!ready) return;
        long nowTime = System.currentTimeMillis();
        if (nowTime < nextAllowReloadTime) return;
        nextAllowReloadTime = nowTime + eventJobProperties.getReloadInterval().toMillis();
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
            JobContext jobContext = removeJobContext(group, name);
            if (jobContext == null) return;
            jobContext.stop();
        } else {
            reload(eventJob);
        }
    }

    private void reload(@NonNull EventJob eventJob) {
        if (!ready) return;
        JobContext jobContext = getJobContext(eventJob);
        jobContext.setEventJob(eventJob);
        addSchedule(jobContext);
    }

    private boolean isCancel(@NonNull JobContext jobContext) {
        return executor == null || jobContext.isCancel();
    }

    /**
     * add job to schedule
     */
    private void addSchedule(@NonNull JobContext jobContext) {
        if (isCancel(jobContext) || jobContext.isWaiting()) return;
        if (executor.isShutdown() || executor.isTerminated()) return;
        EventJob eventJob = jobContext.getEventJob();
        if (StringUtils.hasLength(eventJob.getCron())) {
            jobContext.setScheduledFuture(executor.schedule(() -> {
                if (doExecute(jobContext, false)) {
                    addSchedule(jobContext);
                }
            }, JobUtil.getNextDelay(eventJob.getCron(), eventJob.getTimezone()), TimeUnit.MILLISECONDS));
        } else {
            afterEventJobsMap
                    .computeIfAbsent(eventJob.getAfterGroup(), (key) -> new ConcurrentHashMap<>())
                    .computeIfAbsent(eventJob.getAfterName(), (key) -> new ConcurrentHashMap<>())
                    .put(eventJob.getGroup(), eventJob.getName())
            ;
        }
    }

    private boolean doExecute(@NonNull JobContext jobContext, boolean force) {
        EventJob eventJob = jobContext.getEventJob();
        List<JobHandler> jobHandlers = getJobHandlers(eventJob.getEvent());
        if (jobHandlers.size() == 0) return true;
        if (!force && isCancel(jobContext)) return false;
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
            if (!force && !jobStatus.getEnabled()) return false;
            Long nextTime = getOrFindNextTime(eventJob);
            if (Objects.equals(EventJobStatus.EXECUTING, jobStatus.getStatus())
                    && nextTime.compareTo(jobStatus.getNextTime()) > 0
            ) {
                checkJobReallyInProgress(jobContext, nextTime);
                return false;
            }

            if (!force) {
                int compete = jobStore.compete(instance, group, name, nextTime, System.currentTimeMillis());
                if (compete == 0) return true;
            }

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
            if (executed) executeAfterJob(eventJob.getGroup(), eventJob.getName());
        }
        return false;
    }

    private void checkJobReallyInProgress(@NonNull JobContext jobContext, Long nextTime) {
        if (jobContext.isRunning()) return;
        log.warn("Check that the job is actually being executed.({})", jobContext.getEventJob());
        String token = UUID.randomUUID().toString();
        if (executor.isShutdown() || executor.isTerminated()) return;
        jobContext.pause();
        checkMap.put(token, token);
        notifyCheckAsk(token, jobContext.getEventJob().getGroup(), jobContext.getEventJob().getName());
        executor.schedule(() -> {
            try {
                if (checkMap.remove(token) != null) {
                    tryRelease(jobContext, nextTime);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            jobContext.proceed();
            addSchedule(jobContext);
        }, eventJobProperties.getCheckWaitTime(), TimeUnit.MILLISECONDS);
    }

    private void tryRelease(JobContext jobContext, Long nextTime) {
        log.warn("Revert job status.({})", jobContext.getEventJob());
        int release = jobStore.release(
                getInstance()
                , jobContext.getEventJob().getGroup()
                , jobContext.getEventJob().getName()
                , nextTime
        );
        if (release == 0) return;
        doExecute(jobContext, false);
    }

    private Long getOrFindNextTime(EventJob eventJob) {
        if (StringUtils.hasLength(eventJob.getCron())) {
            return JobUtil.getNextTime(eventJob.getCron(), eventJob.getTimezone());
        } else if (StringUtils.hasLength(eventJob.getAfterGroup())) {
            return getOrFindNextTime(jobStore.findDependentJob(getInstance(), eventJob.getAfterGroup(), eventJob.getAfterName()));
        }
        return 0L;
    }

    private Long getOrFindNextTime(DependentJob dependentJob) {
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
        return eventJobHandlersMap.computeIfAbsent(event, (key) -> Collections.emptyList());
    }

    private void executeJobHandler(String group) {
        if (eventJobHandlersMap.size() == 0) return;
        List<EventJob> eventJobs = jobStore.findAll(getInstance(), group);
        for (EventJob eventJob : eventJobs) {
            executeJobHandler(eventJob.getGroup(), eventJob.getName());
        }
    }

    private void executeJobHandler(String group, String name) {
        JobContext jobContext = getJobContext(group, name);
        if (jobContext == null) return;
        doExecute(jobContext, true);
    }

    private void completeJobHandler(String group, String name) {
        Map<String, String> map = afterEventJobsMap.getOrDefault(group, Collections.emptyMap()).getOrDefault(name, Collections.emptyMap());
        for (Iterator<Map.Entry<String, String>> it = map.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, String> entry = it.next();
            JobContext jobContext = getJobContext(entry.getKey(), entry.getValue());
            if (jobContext == null
                    || StringUtils.hasLength(jobContext.getEventJob().getCron())
            ) {
                it.remove();
                continue;
            }
            doExecute(jobContext, false);
        }
    }

    @NonNull
    private JobContext getJobContext(@NonNull EventJob eventJob) {
        return jobGroupContextMap
                .computeIfAbsent(eventJob.getGroup(), (key) -> new ConcurrentHashMap<>())
                .computeIfAbsent(eventJob.getName(), (key) -> new JobContext(eventJob))
                ;
    }

    private JobContext getJobContext(String group, String name) {
        ConcurrentMap<String, JobContext> groupContexts = jobGroupContextMap.get(group);
        if (groupContexts == null) return null;
        return groupContexts.get(name);
    }

    private JobContext removeJobContext(String group, String name) {
        ConcurrentMap<String, JobContext> groupContexts = jobGroupContextMap.get(group);
        if (groupContexts == null) return null;
        return groupContexts.remove(name);
    }

    private boolean hasChange(@NonNull EventJob job, @NonNull EventJob eventJob) {
        return Objects.equals(job.getGroup(), eventJob.getGroup())
                && Objects.equals(job.getName(), eventJob.getName())
                && Objects.equals(job.getEvent(), eventJob.getEvent())
                && Objects.equals(job.getDescription(), eventJob.getDescription())
                && Objects.equals(job.getTimezone(), eventJob.getTimezone())
                && Objects.equals(job.getCron(), eventJob.getCron())
                && Objects.equals(job.getEnabled(), eventJob.getEnabled())
                && Objects.equals(job.getData(), eventJob.getData());
    }

    private void notifyChange(@NonNull String group) {
        notifyEvent(EventJobEventType.CHANGE, group);
    }

    private void notifyChange(@NonNull String group, String name) {
        notifyEvent(EventJobEventType.CHANGE, group, name);
    }

    private void eventHandler(String body) {
        String[] array = splitMessage(body);
        if (array.length < 1) return;
        String type = array[0];
        switch (type) {
            case EventJobEventType.CHECK:
                checkAskEventHandler(array[1], array[2], array[3]);
                break;
            case EventJobEventType.REPLY:
                checkReplayEventHandler(array[1]);
                break;
            case EventJobEventType.CHANGE:
                changeEventHandler(array[1], array[2]);
                break;
            case EventJobEventType.EXECUTE:
                executeEventHandler(array[1], array[2]);
                break;
            case EventJobEventType.COMPLETE:
                completeEventHandler(array[1], array[2]);
                break;
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

    private void notifyExecute(@NonNull String group) {
        notifyEvent(EventJobEventType.EXECUTE, group);
    }

    private void notifyExecute(@NonNull String group, @NonNull String name) {
        notifyEvent(EventJobEventType.EXECUTE, group, name);
    }

    private void executeEventHandler(String group, String name) {
        try {
            if (name == null) {
                executeJobHandler(group);
            } else {
                executeJobHandler(group, name);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void notifyComplete(@NonNull String group, @NonNull String name) {
        notifyEvent(EventJobEventType.COMPLETE, group, name);
    }

    private void completeEventHandler(String group, String name) {
        try {
            completeJobHandler(group, name);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void notifyCheckAsk(@NonNull String token, @NonNull String group, @NonNull String name) {
        notifyEvent(EventJobEventType.CHECK, group, name, token);
    }

    private void notifyCheckReply(@NonNull String token) {
        notifyEvent(EventJobEventType.REPLY, token);
    }

    private void checkAskEventHandler(String group, String name, String token) {
        try {
            JobContext jobContext = getJobContext(group, name);
            if (jobContext == null) return;
            if (jobContext.isRunning()) {
                notifyCheckReply(token);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void checkReplayEventHandler(String token) {
        try {
            checkMap.remove(token);
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
        return ((com.sun.management.OperatingSystemMXBean) ManagementFactory
                .getOperatingSystemMXBean()).getSystemCpuLoad();
    }

    private void notifyEvent(String type, String... args) {
        if (eventJobMessageService == null) return;
        StringBuilder message = new StringBuilder(type);
        for (String arg : args) {
            message.append(eventSplit).append(arg);
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


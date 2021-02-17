package pers.clare.core.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import pers.clare.core.scheduler.exception.JobException;
import pers.clare.core.scheduler.exception.JobNotExistException;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Log4j2
public class JdbcScheduler implements Scheduler {
    static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

    private final ConcurrentMap<String, ConcurrentMap<String, JobContext>> jobGroupContexts = new ConcurrentHashMap<>();

    @Getter
    private final String instance;

    @Getter(AccessLevel.PACKAGE)
    private JobStore jobStore;

    private long reloadTime = 60 * 1000;

    public JdbcScheduler(DataSource dataSource) {
        this("scheduler", dataSource);
    }

    public JdbcScheduler(
            String instance
            , DataSource dataSource
    ) {
        this.instance = instance;
        this.jobStore = new JdbcJobStore(dataSource);
    }

    @Override
    public void run(String... args) throws Exception {
        executor.scheduleAtFixedRate(this::reload, 0, reloadTime, TimeUnit.MILLISECONDS);
    }

    ScheduledFuture schedule(Runnable command, long delay, TimeUnit unit) {
        return executor.schedule(command, delay, unit);
    }

    private void reload() {
        try {
            log.info("reload");
            List<EventJob> eventJobs = jobStore.findAll(instance);
            for (EventJob eventJob : eventJobs) {
                reload(eventJob);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void reload(String group, String name) throws JobException {
        log.info("reload1 " + group + ":" + name);
        try{
            reload(jobStore.find(instance, group, name));
        }catch (Exception e){
            throw new JobException(e);
        }
    }

    private void reload(EventJob eventJob) {
        log.info("reload2 ");
        JobContext jobContext = getJobContext(eventJob.getGroup(), eventJob.getName());
        if (jobContext == null) return;
        jobContext.reload(eventJob);
        schedule(jobContext);
    }

    private void schedule(JobContext jobContext) {
        EventJob eventJob = jobContext.getEventJob();
        if (eventJob == null || !eventJob.getEnabled()) return;
        executor.schedule(() -> {
            long nowTime = System.currentTimeMillis();
            Long nextTime = JobUtil.getNextTime(eventJob.getCron(), eventJob.getTimezone());
            eventJob.setNextTime(nextTime);
            eventJob.setPrevTime(nowTime);
            try {
                jobStore.executeLock(instance, jobContext.getEventJob(), jobContext::execute);
                schedule(jobContext);
            } catch (JobNotExistException e) {
                reload(eventJob.getGroup(),eventJob.getName());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                schedule(jobContext);
            }
        }, eventJob.getNextTime() - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    public void handler(String group, String name, Consumer<EventJob> consumer) {
        ConcurrentMap<String, JobContext> jobContext = jobGroupContexts.get(group);
        if (jobContext == null) {
            jobGroupContexts.put(group, jobContext = new ConcurrentHashMap<>());
        }
        JobContext context = jobContext.get(name);
        if (context == null) {
            jobContext.put(name, context = new JobContext(this));
        }
        context.addConsumer(consumer);
    }

    public void add(Job job) throws RuntimeException {
            String group = job.getGroup();
            String name = job.getName();
            Long nextTime = JobUtil.getNextTime(job.getCron(), job.getTimezone());
            EventJob eventJob = jobStore.find(instance, group, name);
            if (eventJob == null) {
                jobStore.insert(instance, job, nextTime);
                reload(group, name);
            } else if (!equals(job, eventJob)) {
                jobStore.update(instance, job, nextTime);
                reload(eventJob);
            }
    }

    public void remove(String group, String name) throws RuntimeException {
        try {
            jobStore.delete(instance, group, name);
            reload(group, name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void enable(String group, String name) throws RuntimeException {

    }

    @Override
    public void disable(String group, String name) throws RuntimeException {

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
}


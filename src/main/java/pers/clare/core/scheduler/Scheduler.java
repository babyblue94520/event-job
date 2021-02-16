package pers.clare.core.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.support.CronSequenceGenerator;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Log4j2
public class Scheduler implements CommandLineRunner{
    protected static final String eventSplit = ",";
    @Getter
    private final String instance;
    @Getter
    private final String topic;

    private final ConcurrentMap<String, ConcurrentMap<String, JobContext>> jobGroupContexts = new ConcurrentHashMap<>();

    @Getter(AccessLevel.PACKAGE)
    private JobStore jobStore;

    private ScheduleMQService scheduleMQService;

    private boolean init = false;

    public Scheduler() {
        this("scheduler", null, null, null);
    }

    public Scheduler(JobStore jobStore, String topic, ScheduleMQService scheduleMQService) {
        this("scheduler", jobStore, topic, scheduleMQService);
    }

    public Scheduler(
            String instance
            , JobStore jobStore
            , String topic
            , ScheduleMQService scheduleMQService
    ) {
        this.instance = instance;
        this.topic = topic == null ? null : topic + "." + instance;
        this.jobStore = jobStore;
        this.scheduleMQService = scheduleMQService;
    }

    @Override
    public void run(String... args) throws Exception {
        if (scheduleMQService == null) {
            reload();
        }else{
            scheduleMQService.onConnected(() -> {
                try {
                    reload();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
            scheduleMQService.addListener(topic, (body) -> {
                String[] data = body.split(eventSplit);
                if (data.length != 2) return;
                String group = data[0];
                String name = data[1];

                try {
                    reload(group, name);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });

        }
    }

    private void reload() throws SQLException, JsonProcessingException {
        log.info("reload");
        List<EventJob> eventJobs = jobStore.findAll(instance);
        for(EventJob eventJob : eventJobs){
            JobContext jobContext =getJobContext(eventJob.getGroup(),eventJob.getName());
            if (jobContext == null) continue;
            jobContext.reload(eventJob);
        }
    }

    private void reload(String group, String name) throws SQLException, JsonProcessingException {
        log.info("reload1 " + group + ":" + name);
        JobContext jobContext = getJobContext(group, name);
        if (jobContext == null) return;
        jobContext.reload(jobStore.find(instance, group, name));
    }

    private void reload(EventJob eventJob) {
        log.info("reload2 ");
        JobContext jobContext = getJobContext(eventJob.getGroup(), eventJob.getName());
        if (jobContext == null) return;
        jobContext.reload(eventJob);
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

    public void add(Job job) throws SQLException, JsonProcessingException {
        String group = job.getGroup();
        String name = job.getName();
        CronSequenceGenerator cronSequenceGenerator = JobUtil.buildCronGenerator(job.getCron(), job.getTimezone());
        long nextTime = JobUtil.getNextTime(cronSequenceGenerator);
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

    public void remove(String group, String name) throws SQLException, JsonProcessingException {
        jobStore.delete(instance, group, name);
        reload(group, name);
        notifyChange(group, name);
    }

    private JobContext getJobContext(String group, String name) {
        ConcurrentMap<String, JobContext> jobContexts = jobGroupContexts.get(group);
        if (jobContexts == null) return null;
        JobContext context = jobContexts.get(name);
        return context;
    }

    private void notifyChange(String group, String name) {
        if (scheduleMQService == null) return;
        scheduleMQService.send(topic, group + eventSplit + name);
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


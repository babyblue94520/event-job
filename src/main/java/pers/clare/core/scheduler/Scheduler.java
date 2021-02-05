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
public class Scheduler implements CommandLineRunner, InitializingBean {
    protected static final String eventSplit = ",";
    @Getter
    private final String instance;
    @Getter
    private final String topic;

    @Getter
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

    private final ConcurrentMap<String, ConcurrentMap<String, JobContext>> jobGroupContexts = new ConcurrentHashMap<>();

    @Getter(AccessLevel.PACKAGE)
    private JobStore jobStore;

    @Getter(AccessLevel.PACKAGE)
    private ScheduleMQService scheduleMQService;

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
    public void afterPropertiesSet() throws Exception {
        if (scheduleMQService != null) {
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

            scheduleMQService.onConnected(() -> {
                try {
                    reload();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
    }

    @Override
    public void run(String... args) throws Exception {
        reload();
    }

    private void reload() throws SQLException, JsonProcessingException {
        log.info("reload");
        for (Map.Entry<String, ConcurrentMap<String, JobContext>> groupEntry : jobGroupContexts.entrySet()) {
            for (Map.Entry<String, JobContext> entry : groupEntry.getValue().entrySet()) {
                entry.getValue().setEventJob(jobStore.find(instance, groupEntry.getKey(), entry.getKey()));
            }
        }
    }

    private void reload(String group, String name) throws SQLException, JsonProcessingException {
        JobContext jobContext = getJobContext(group, name);
        if (jobContext == null) return;
        jobContext.setEventJob(jobStore.find(instance, group, name));
    }

    public void handler(String group, String name, Consumer<EventJob> consumer) {
        ConcurrentMap<String, JobContext> jobContext = jobGroupContexts.get(group);
        if (jobContext == null) {
            jobGroupContexts.put(group, jobContext = new ConcurrentHashMap<>());
        }
        JobContext context = jobContext.get(name);
        if (context == null) {
            jobContext.put(name, context = new JobContext(this, new ArrayList<>()));
        }
        context.addConsumer(consumer);
    }

    public void add(Job job) throws SQLException, JsonProcessingException {
        String group = job.getGroup();
        String name = job.getName();
        CronSequenceGenerator cronSequenceGenerator = JobUtil.buildCronGenerator(job.getCron(), job.getTimezone());
        long nextTime = JobUtil.getNextTime(cronSequenceGenerator);
        try {
            if (jobStore.exists(instance, group, name)) {
                jobStore.update(instance, job, nextTime);
            } else {
                jobStore.insert(instance, job, nextTime);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        reload(group, name);
        notifyChange(group, name);
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
}


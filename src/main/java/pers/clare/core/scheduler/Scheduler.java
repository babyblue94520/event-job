package pers.clare.core.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.log4j.Log4j2;
import pers.clare.core.scheduler.bo.Job;
import pers.clare.core.scheduler.bo.EventJob;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Log4j2
public class Scheduler {
    private JobStore jobStore;
    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(3);

    private String instance = "scheduler";

    private Map<String, Map<String, JobContext>> jobGroupContexts = new ConcurrentHashMap<>();

    private void load() throws SQLException, JsonProcessingException {
        for (Map.Entry<String, Map<String, JobContext>> groupEntry : jobGroupContexts.entrySet()) {
            for (Map.Entry<String, JobContext> entry : groupEntry.getValue().entrySet()) {
                EventJob eventJob = jobStore.find(instance, groupEntry.getKey(), entry.getKey());
                entry.getValue().setEventJob(eventJob);
            }
        }
    }

    public void add(Job job) throws SQLException {
        if (jobStore.exists(instance, job.getGroup(), job.getName())) {
        } else {
        }
    }

    public void update(Job job) {

    }

    public void remove(String group, String name) {

    }

    void on(String group, String name, Consumer<EventJob> consumer) {
        Map<String, JobContext> jobContext = jobGroupContexts.get(group);
        if (jobContext == null) {
            jobGroupContexts.put(group, jobContext = new HashMap<>());
        }
        JobContext context = jobContext.get(name);
        if (context == null) {
            jobContext.put(name, context = new JobContext(this, new ArrayList<>()));
        }
        context.addConsumer(consumer);
    }

    private JobContext getJobContext(String group, String name) {
        Map<String, JobContext> jobContexts = jobGroupContexts.get(group);
        if (jobContexts == null) return null;
        JobContext context = jobContexts.get(name);
        return context;
    }

}


package pers.clare.eventjob.impl;

import pers.clare.eventjob.Job;
import pers.clare.eventjob.JobStore;
import pers.clare.eventjob.constant.EventJobStatus;
import pers.clare.eventjob.exception.JobException;
import pers.clare.eventjob.function.JobExecutor;

import java.util.*;
import java.util.concurrent.*;

public class LocalJobStoreImpl implements JobStore {
    private final ConcurrentMap<String, ConcurrentMap<String, EventJob>> eventJobs = new ConcurrentHashMap<>();

    @Override
    public List<EventJob> findAll(String instance) throws JobException {
        List<EventJob> result = new ArrayList<>();
        for (ConcurrentMap<String, EventJob> jobs : eventJobs.values()) {
            result.addAll(jobs.values());
        }
        return result;
    }

    @Override
    public List<EventJob> findAll(String instance, String group) throws JobException {
        ConcurrentMap<String, EventJob> jobs = eventJobs.get(group);
        return jobs == null ? Collections.emptyList() : new ArrayList<>(jobs.values());
    }

    @Override
    public EventJob find(String instance, String group, String name) throws JobException {
        ConcurrentMap<String, EventJob> jobs = eventJobs.get(group);
        if (jobs == null) return null;
        return jobs.get(name);
    }

    @Override
    public void insert(String instance, Job job, long nextTime) throws JobException {
        String group = job.getGroup();
        eventJobs.computeIfAbsent(group, (key) -> new ConcurrentHashMap<>())
                .put(job.getName(), new EventJob(
                        instance
                        , job.getGroup()
                        , job.getName()
                        , job.getEvent()
                        , job.getTimezone()
                        , job.getDescription()
                        , job.getCron()
                        , EventJobStatus.WAITING
                        , 0L
                        , nextTime
                        , 0L
                        , 0L
                        , true
                        , job.getData()
                ));
    }

    @Override
    public void update(String instance, Job job, long nextTime) throws JobException {
        EventJob eventJob = find(instance, job.getGroup(), job.getName());
        if (eventJob == null) return;
        eventJob.setTimezone(job.getTimezone());
        eventJob.setDescription(job.getDescription());
        eventJob.setCron(job.getCron());
        eventJob.setNextTime(nextTime);
        eventJob.setEnabled(job.getEnabled());
        eventJob.setData(job.getData());
    }

    @Override
    public void delete(String instance, String group) throws JobException {
        eventJobs.remove(group);
    }

    @Override
    public void delete(String instance, String group, String name) throws JobException {
        ConcurrentMap<String, EventJob> jobs = eventJobs.get(group);
        if (jobs == null) return;
        jobs.remove(name);
    }

    @Override
    public void enable(String instance, String group) throws JobException {
        ConcurrentMap<String, EventJob> jobs = eventJobs.get(group);
        if (jobs == null) return;
        for (Map.Entry<String, EventJob> jobEntry : jobs.entrySet()) {
            jobEntry.getValue().setEnabled(true);
        }
    }

    @Override
    public void enable(String instance, String group, String name) throws JobException {
        EventJob eventJob = find(instance, group, name);
        if (eventJob == null) return;
        eventJob.setEnabled(true);
    }

    @Override
    public void disable(String instance, String group) throws JobException {
        ConcurrentMap<String, EventJob> jobs = eventJobs.get(group);
        if (jobs == null) return;
        for (Map.Entry<String, EventJob> jobEntry : jobs.entrySet()) {
            jobEntry.getValue().setEnabled(false);
        }
    }

    @Override
    public void disable(String instance, String group, String name) throws JobException {
        EventJob eventJob = find(instance, group, name);
        if (eventJob == null) return;
        eventJob.setEnabled(false);
    }

    @Override
    public int executor(
            EventJob eventJob
            , JobExecutor executor
    ) throws JobException {
        if (eventJob == null || !eventJob.getEnabled()) return 0;
        executor.execute(eventJob);
        eventJob.setEndTime(System.currentTimeMillis());
        return 1;
    }
}

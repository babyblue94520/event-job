package pers.clare.eventjob;

import pers.clare.eventjob.exception.JobException;
import pers.clare.eventjob.function.JobExecutor;
import pers.clare.eventjob.impl.EventJob;

import java.util.List;

public interface JobStore {

    List<EventJob> findAll(String instance) throws JobException;

    List<EventJob> findAll(String instance, String group) throws JobException;

    EventJob find(String instance, String group, String name) throws JobException;

    void insert(String instance, Job job, long nextTime) throws JobException;

    void update(String instance, Job job, long nextTime) throws JobException;

    void delete(String instance, String group) throws JobException;

    void delete(String instance, String group, String name) throws JobException;

    void enable(String instance, String group) throws JobException;

    void enable(String instance, String group, String name) throws JobException;

    void disable(String instance, String group) throws JobException;

    void disable(String instance, String group, String name) throws JobException;

    int executor(
            EventJob eventJob
            , JobExecutor executor
    ) throws JobException;
}

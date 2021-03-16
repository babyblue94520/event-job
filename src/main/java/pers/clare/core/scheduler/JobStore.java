package pers.clare.core.scheduler;

import pers.clare.core.scheduler.exception.JobException;
import pers.clare.core.scheduler.impl.EventJob;

import java.util.List;

public interface JobStore {

    List<EventJob> findAll(String instance) throws JobException;

    EventJob find(String instance, String group, String name) throws JobException;

    void insert(String instance, Job job, long nextTime) throws JobException;

    void update(String instance, Job job, long nextTime) throws JobException;

    void delete(String instance, String group, String name) throws JobException;

    void enable(String instance, String group, String name) throws JobException;

    void disable(String instance, String group, String name) throws JobException;

    int executor(
            EventJob eventJob
            , Runnable runnable
    ) throws JobException;
}

package pers.clare.core.scheduler;

import pers.clare.core.scheduler.exception.JobException;

import java.util.List;

public interface JobStore {

    Boolean exists(String instance, String group, String name) throws JobException;

    List<EventJob> findAll(String instance) throws JobException;

    EventJob find(String instance, String group, String name) throws JobException;

    void insert(String instance, Job job, long nextTime) throws JobException;

    void update(String instance, Job job, long nextTime) throws JobException;

    void delete(String instance, String group, String name) throws JobException;

    void executeLock(
            String instance
            , EventJob eventJob
            , Runnable runnable
    ) throws Exception;
}

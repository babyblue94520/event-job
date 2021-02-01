package pers.clare.core.scheduler;

import pers.clare.core.scheduler.bo.Job;

import java.util.List;

public interface JobStore {

    List<Job> findAll(String instance);

    Job find(String instance, String group, String name);

    void insert(Job job);

    void update(Job job);

    void delete(String instance, String group, String name);
}

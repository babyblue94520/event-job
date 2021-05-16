package pers.clare.eventjob;


import pers.clare.eventjob.function.JobExecutor;

public interface Scheduler {

    /**
     * create or modify job
     */
    void add(Job job);

    void remove(String group);

    void remove(String group, String name);

    /**
     * start all job by group
     */
    void enable(String group);

    /**
     * start job
     */
    void enable(String group, String name);

    /**
     * stop all job by group
     */
    void disable(String group);

    /**
     * stop job
     */
    void disable(String group, String name);

    /**
     * add job event executor
     */
    JobExecutor addExecutor(String event, JobExecutor jobExecutor);

    /**
     * remove job event executor
     */
    void removeExecutor(String event, JobExecutor jobExecutor);

    /**
     * executor all job by group
     */
    void execute(String group);

    /**
     * executor job
     */
    void execute(String group, String name);

}


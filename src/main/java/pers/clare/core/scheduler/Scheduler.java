package pers.clare.core.scheduler;


public interface Scheduler {

    void register(String group, String name, JobExecutor jobExecutor);

    void add(Job job) throws RuntimeException;

    void remove(String group, String name) throws RuntimeException;

    void enable(String group, String name) throws RuntimeException;

    void disable(String group, String name) throws RuntimeException;

}


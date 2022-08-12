package pers.clare.eventjob;


import org.springframework.lang.NonNull;
import pers.clare.eventjob.function.JobHandler;
import pers.clare.eventjob.impl.EventJob;

import java.util.List;

@SuppressWarnings("unused")
public interface EventScheduler {

    String getInstance();

    @NonNull
    List<EventJob> findAll();

    @NonNull
    List<EventJob> findAll(@NonNull String group);

    EventJob find(@NonNull String group, @NonNull String name);

    /**
     * create or modify job
     */
    void add(@NonNull EventJob job);

    void remove(@NonNull String group);

    void remove(@NonNull String group, @NonNull String name);

    /**
     * start all job by group
     */
    void enable(@NonNull String group);

    /**
     * start job
     */
    void enable(@NonNull String group, @NonNull String name);

    /**
     * stop all job by group
     */
    void disable(@NonNull String group);

    /**
     * stop job
     */
    void disable(@NonNull String group, @NonNull String name);

    /**
     * add job event executor
     */
    JobHandler addHandler(@NonNull String event, @NonNull JobHandler jobHandler);

    /**
     * remove job event executor
     */
    void removeHandler(@NonNull String event, @NonNull JobHandler jobHandler);

    /**
     * executor all job by group
     */
    void execute(@NonNull String group);

    /**
     * executor job
     */
    void execute(@NonNull String group, @NonNull String name);

}


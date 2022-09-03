package pers.clare.eventjob;

import pers.clare.eventjob.vo.DependentJob;
import pers.clare.eventjob.vo.EventJob;
import org.springframework.lang.NonNull;
import pers.clare.eventjob.exception.JobException;

import java.util.List;

@SuppressWarnings("UnusedReturnValue")
public interface JobStore {
    @NonNull
    List<EventJob> findAll(String instance) throws JobException;

    @NonNull
    List<EventJob> findAll(String instance, String group) throws JobException;

    EventJob find(@NonNull String instance, @NonNull String group, @NonNull String name) throws JobException;

    DependentJob findDependentJob(@NonNull String instance, @NonNull String group, @NonNull String name) throws JobException;

    void insert(@NonNull String instance, @NonNull EventJob job, @NonNull long nextTime) throws JobException;

    void update(@NonNull String instance, @NonNull EventJob job, @NonNull long nextTime) throws JobException;

    void delete(@NonNull String instance, @NonNull String group) throws JobException;

    void delete(@NonNull String instance, @NonNull String group, @NonNull String name) throws JobException;

    void enable(@NonNull String instance, @NonNull String group) throws JobException;

    void enable(@NonNull String instance, @NonNull String group, @NonNull String name) throws JobException;

    void disable(@NonNull String instance, @NonNull String group) throws JobException;

    void disable(@NonNull String instance, @NonNull String group, @NonNull String name) throws JobException;

    JobStatus getStatus(@NonNull String instance, @NonNull String group, @NonNull String name);

    @NonNull
    int release(@NonNull String instance, @NonNull String group, @NonNull String name, @NonNull long nextTime);

    @NonNull
    int compete(@NonNull String instance, @NonNull String group, @NonNull String name
            , @NonNull long nextTime, @NonNull long startTime);

    @NonNull
    int finish(@NonNull String instance, @NonNull String group, @NonNull String name, @NonNull long endTime);

}

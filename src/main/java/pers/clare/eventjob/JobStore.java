package pers.clare.eventjob;

import pers.clare.eventjob.vo.EventJob;
import org.springframework.lang.NonNull;
import pers.clare.eventjob.exception.JobException;

import java.util.List;

@SuppressWarnings("UnusedReturnValue")
public interface JobStore {
    @NonNull
    List<EventJob> findAll(String instance);

    @NonNull
    List<EventJob> findAll(String instance, String group);

    EventJob find(@NonNull String instance, @NonNull String group, @NonNull String name);

    void insert(@NonNull String instance, @NonNull EventJob job, @NonNull long nextTime);

    void update(@NonNull String instance, @NonNull EventJob job, @NonNull long nextTime);

    void updateActive(@NonNull String instance, @NonNull EventJob job, @NonNull long activeTime);

    void delete(@NonNull String instance, @NonNull String group);

    void delete(@NonNull String instance, @NonNull String group, @NonNull String name);

    void enable(@NonNull String instance, @NonNull String group);

    void enable(@NonNull String instance, @NonNull String group, @NonNull String name);

    void disable(@NonNull String instance, @NonNull String group);

    void disable(@NonNull String instance, @NonNull String group, @NonNull String name);

    JobStatus getStatus(@NonNull String instance, @NonNull String group, @NonNull String name);

    @NonNull
    int release(@NonNull String instance, @NonNull String group, @NonNull String name, @NonNull long nextTime);

    @NonNull
    int compete(@NonNull String instance, @NonNull String group, @NonNull String name
            , @NonNull long nextTime, @NonNull long startTime);

    /**
     * Used to execute instructions.
     */
    int compete(
            String instance, String group, String name
            , long startTime
    );

    @NonNull
    int finish(@NonNull String instance, @NonNull String group, @NonNull String name, @NonNull long endTime);

}

package pers.clare.eventjob.function;

import pers.clare.eventjob.impl.EventJob;

public interface JobExecutor {
    void execute(EventJob eventJob);
}

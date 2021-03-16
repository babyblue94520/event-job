package pers.clare.core.scheduler;

import pers.clare.core.scheduler.impl.EventJob;

public interface JobExecutor {
    void execute(EventJob eventJob);
}

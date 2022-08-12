package pers.clare.eventjob.function;

import pers.clare.eventjob.impl.EventJob;

public interface JobHandler {
    void execute(EventJob eventJob);
}

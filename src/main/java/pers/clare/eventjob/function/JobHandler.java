package pers.clare.eventjob.function;

import pers.clare.eventjob.vo.EventJob;

public interface JobHandler {
    void execute(EventJob eventJob);
}

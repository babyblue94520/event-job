package pers.clare.eventjob.impl;

import org.springframework.beans.factory.InitializingBean;
import pers.clare.eventjob.function.JobExecutor;

public abstract class AbstractJobExecutor implements JobExecutor, InitializingBean {
    protected final String event;

    protected AbstractJobExecutor(String event) {
        this.event = event;
    }

    @Override
    public void afterPropertiesSet() throws Exception {

    }
}

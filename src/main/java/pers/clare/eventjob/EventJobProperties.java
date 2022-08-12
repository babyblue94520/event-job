package pers.clare.eventjob;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@ConfigurationProperties(prefix = EventJobProperties.PREFIX)
public class EventJobProperties {
    public static final String PREFIX = "event-job";

    private String instance = "eventJobScheduler";

    private String topic = "event.job";

    private Integer threadCount = 1;

    /**
     * Reload all job intervals.
     */
    private Duration reloadInterval = Duration.parse("PT60S");

    /**
     * The time is to check that the job is actually being executed.
     */
    private Long checkWaitTime = 1000L;

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(Integer threadCount) {
        this.threadCount = threadCount;
    }

    public Duration getReloadInterval() {
        return reloadInterval;
    }

    public void setReloadInterval(Duration reloadInterval) {
        this.reloadInterval = reloadInterval;
    }

    public Long getCheckWaitTime() {
        return checkWaitTime;
    }

    public void setCheckWaitTime(Long checkWaitTime) {
        this.checkWaitTime = checkWaitTime;
    }
}

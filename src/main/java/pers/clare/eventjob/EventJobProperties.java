package pers.clare.eventjob;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = EventJobProperties.PREFIX)
public class EventJobProperties {
    public static final String PREFIX = "event-job";

    private String instance = "eventJobScheduler";

    private Integer threadCount = 1;

    /**
     * Reload all job intervals.
     */
    private Duration reloadInterval = Duration.parse("PT60S");

    /**
     * The time is to check that the job is actually being executed.
     */
    private Long checkWaitTime = 1000L;

    /**
     * The running job periodically updates its last active timestamp.
     */
    private Duration updateActiveInterval = Duration.parse("PT60S");

}

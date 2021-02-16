package pers.clare.core.scheduler;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;
import java.util.Objects;

@Getter
@AllArgsConstructor
public class EventJob {
    private String instance;
    private String group;
    private String name;
    private String timezone;
    private String description;
    private String cron;
    @Setter(AccessLevel.PACKAGE)
    private Long prevTime;
    @Setter(AccessLevel.PACKAGE)
    private Long nextTime;
    private Boolean enabled;
    private Map<String, Object> data;

}

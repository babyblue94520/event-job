package pers.clare.test.eventjob;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import pers.clare.eventjob.EnableEventJob;
import pers.clare.eventjob.EventScheduler;
import pers.clare.eventjob.vo.EventJob;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@EnableEventJob
@Log4j2
@Configuration
@RequiredArgsConstructor
public class EventJobRegister implements InitializingBean {
    private static final ConcurrentHashMap<String, ConcurrentHashMap<EventJob, AtomicInteger>> eventJobCountMap = new ConcurrentHashMap<>();

    private final String service = UUID.randomUUID().toString();

    private final EventScheduler eventScheduler;

    private final ConcurrentHashMap<EventJob, AtomicInteger> countMap = new ConcurrentHashMap<>();

    {
        eventJobCountMap.put(service, countMap);
    }

    @Override
    public void afterPropertiesSet() {
        List<EventJob> eventJobs = eventScheduler.findAll();

        log.info("event job count: {}", eventJobs.size());

        eventJobs.forEach(eventJob -> {
            countMap.put(eventJob, new AtomicInteger(0));

            eventScheduler.addHandler(eventJob.getEvent(), (eventJob2) -> {
                countMap.computeIfAbsent(eventJob2, (key) -> new AtomicInteger(0))
                        .incrementAndGet();
            });
        });
    }

    public static int getCount(EventJob job) {
        int count = 0;
        for (ConcurrentHashMap<EventJob, AtomicInteger> map : eventJobCountMap.values()) {
            var increment = map.get(job);
            if (increment != null) {
                count += increment.get();
            }
        }
        return count;
    }

    public static void reset(){
        for (ConcurrentHashMap<EventJob, AtomicInteger> map : eventJobCountMap.values()) {
            for (AtomicInteger value : map.values()) {
                value.set(0);
            }
        }
    }
}

package pers.clare.test.eventjob;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import pers.clare.eventjob.EnableEventJob;
import pers.clare.eventjob.EventScheduler;

import java.util.UUID;

@EnableEventJob
@Log4j2
@Configuration
public class EventJobRegister implements InitializingBean {
    private final String service = UUID.randomUUID().toString();

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private EventScheduler eventScheduler;

    @Override
    public void afterPropertiesSet() throws Exception {
        eventScheduler.findAll().forEach(eventJob -> {
            jdbcTemplate.update("insert into log values(?,?,?,?,?)"
                    , eventScheduler.getInstance()
                    , eventJob.getGroup()
                    , eventJob.getName()
                    , service
                    , 0
            );

            eventScheduler.addHandler(eventJob.getEvent(), (eventJob2) -> {
                jdbcTemplate.update("update log set `count`=`count`+1 where instance = ? and `group` = ? and name = ? and service = ?"
                        , eventScheduler.getInstance(), eventJob2.getGroup(), eventJob2.getName(), service);
            });
        });
    }
}

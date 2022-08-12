package pers.clare.eventjob.impl;

import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.jdbc.Sql;
import pers.clare.eventjob.EventScheduler;
import pers.clare.eventjob.function.JobHandler;
import pers.clare.eventjob.util.DataSourceSchemaUtil;
import pers.clare.h2.H2Application;
import pers.clare.test.ApplicationTest2;
import pers.clare.test.eventjob.EventJobMessageServiceImpl;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@DisplayName("EventSchedulerImplTest")
@Log4j2
@TestInstance(PER_CLASS)
class EventSchedulerImplTest {

    private void assertZero(Integer count) {
        assertEquals(count, 0, () -> String.format("count: %d", count));
    }

    private void assertGreaterZero(Integer count) {
        assertTrue(count > 0, () -> String.format("count: %d", count));
    }

    static {
        H2Application.main(null);
    }

    @DisplayName("Single")
    @SpringBootTest(
            properties = {"spring.profiles.active=single", "event-job.reload-interval=1s"}
    )
    @Nested
    @TestInstance(PER_CLASS)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class Single {

        private final String tag = String.valueOf(System.currentTimeMillis());
        private final String afterTag = "after-" + System.currentTimeMillis();
        private final String afterTag2 = "after2-" + System.currentTimeMillis();
        private final Map<String, Object> map = new HashMap<>() {
            {
                put("test", "test");
            }
        };
        private final EventJob job = EventJob.builder()
                .group(tag)
                .name(tag)
                .event(tag)
                .cron("*/1 * * * * ?")
                .timezone("+00:00")
                .data(map)
                .build();
        private final EventJob afterJob = EventJob.builder()
                .group(afterTag)
                .name(afterTag)
                .event(afterTag)
                .afterGroup(tag)
                .afterName(tag)
                .timezone("+00:00")
                .data(map)
                .build();
        private final EventJob afterJob2 = EventJob.builder()
                .group(afterTag2)
                .name(afterTag2)
                .event(afterTag2)
                .afterGroup(afterTag)
                .afterName(afterTag)
                .timezone("+00:00")
                .data(map)
                .build();
        private final EventJob sameGroup = EventJob.builder()
                .group(tag)
                .name(tag + "#same2")
                .event(tag + "#same2")
                .cron("*/1 * * * * ?")
                .timezone("+08:00")
                .data(map)
                .build();
        private final EventJob differentGroup = EventJob.builder()
                .group(tag + "3")
                .name(tag)
                .event(tag + "3")
                .cron("*/1 * * * * ?")
                .timezone("+00:00")
                .data(map)
                .build();
        private final AtomicInteger count = new AtomicInteger();
        private final AtomicInteger afterCount = new AtomicInteger();
        private final AtomicInteger afterCount2 = new AtomicInteger();
        private final AtomicInteger sameGroupCount = new AtomicInteger();
        private final AtomicInteger differentGroupCount = new AtomicInteger();
        private final JobHandler jobHandler = (eventJob) -> {
            count.incrementAndGet();
            log.info(eventJob);
        };
        private final JobHandler afterJobHandler = (eventJob) -> {
            afterCount.incrementAndGet();
            log.info(eventJob);
        };
        private final JobHandler afterJobHandler2 = (eventJob) -> {
            afterCount2.incrementAndGet();
            log.info(eventJob);
        };
        private final JobHandler sameGroupHandler = (eventJob) -> {
            sameGroupCount.incrementAndGet();
            log.info(eventJob);
        };
        private final JobHandler differentHandler = (eventJob) -> {
            differentGroupCount.incrementAndGet();
            log.info(eventJob);
        };
        @Autowired
        private EventScheduler eventScheduler;

        private void delay() {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void sleep() {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @BeforeEach
        void before() {
            eventScheduler.add(job);
            eventScheduler.add(afterJob);
            eventScheduler.add(afterJob2);
            eventScheduler.add(sameGroup);
            eventScheduler.add(differentGroup);
            eventScheduler.addHandler(job.getEvent(), jobHandler);
            eventScheduler.addHandler(afterJob.getEvent(), afterJobHandler);
            eventScheduler.addHandler(afterJob2.getEvent(), afterJobHandler2);
            eventScheduler.addHandler(sameGroup.getEvent(), sameGroupHandler);
            eventScheduler.addHandler(differentGroup.getEvent(), differentHandler);
            reset();
        }

        @AfterEach
        void after() {
            eventScheduler.remove(job.getGroup());
            eventScheduler.remove(afterJob.getGroup());
            eventScheduler.remove(sameGroup.getGroup());
            eventScheduler.remove(differentGroup.getGroup());
            eventScheduler.removeHandler(job.getEvent(), jobHandler);
            eventScheduler.removeHandler(afterJob.getEvent(), afterJobHandler);
            eventScheduler.removeHandler(afterJob2.getEvent(), afterJobHandler2);
            eventScheduler.removeHandler(sameGroup.getEvent(), sameGroupHandler);
            eventScheduler.removeHandler(differentGroup.getEvent(), differentHandler);
        }

        private void reset() {
            count.set(0);
            afterCount.set(0);
            afterCount2.set(0);
            sameGroupCount.set(0);
            differentGroupCount.set(0);
        }

        @Test
        @Order(3)
        void disable() {
            eventScheduler.disable(job.getGroup(), job.getName());
            delay();
            reset();
            sleep();
            assertZero(count.get());
            assertGreaterZero(sameGroupCount.get());
            assertGreaterZero(differentGroupCount.get());
            assertZero(afterCount.get());
            assertZero(afterCount2.get());
        }

        @Test
        @Order(4)
        void enable() {
            eventScheduler.enable(job.getGroup(), job.getName());
            reset();
            sleep();
            assertGreaterZero(count.get());
            assertGreaterZero(sameGroupCount.get());
            assertGreaterZero(differentGroupCount.get());
            assertGreaterZero(afterCount.get());
            assertGreaterZero(afterCount2.get());
        }


        @Test
        @Order(5)
        void disableGroup() {
            eventScheduler.disable(job.getGroup());
            delay();
            reset();
            sleep();
            assertZero(count.get());
            assertZero(sameGroupCount.get());
            assertGreaterZero(differentGroupCount.get());
            assertZero(afterCount.get());
            assertZero(afterCount2.get());
        }

        @Test
        @Order(6)
        void enableGroup() {
            eventScheduler.enable(job.getGroup());
            reset();
            sleep();
            assertGreaterZero(count.get());
            assertGreaterZero(sameGroupCount.get());
            assertGreaterZero(differentGroupCount.get());
            assertGreaterZero(afterCount.get());
            assertGreaterZero(afterCount2.get());
        }

        @Test
        @Order(7)
        void removeHandler() {
            reset();
            sleep();
            int c = count.get();
            assertTrue(c > 0, () -> String.format("count: %d", c));
            eventScheduler.removeHandler(job.getEvent(), jobHandler);
            delay();
            reset();
            sleep();
            assertZero(count.get());
            assertGreaterZero(sameGroupCount.get());
            assertGreaterZero(differentGroupCount.get());
            assertZero(afterCount.get());
            assertZero(afterCount2.get());
        }

        @Test
        @Order(8)
        void remove() {
            eventScheduler.remove(job.getGroup(), job.getName());
            delay();
            reset();
            sleep();
            assertZero(count.get());
            assertGreaterZero(sameGroupCount.get());
            assertGreaterZero(differentGroupCount.get());
            assertZero(afterCount.get());
            assertZero(afterCount2.get());
        }

        @Test
        @Order(9)
        void removeGroup() {
            eventScheduler.remove(job.getGroup());
            delay();
            reset();
            sleep();
            assertZero(count.get());
            assertZero(sameGroupCount.get());
            assertGreaterZero(differentGroupCount.get());
            assertZero(afterCount.get());
            assertZero(afterCount2.get());
        }

        @Test
        @Order(10)
        void execute() {
            eventScheduler.disable(job.getGroup());
            delay();
            reset();
            eventScheduler.execute(job.getGroup(), job.getName());
            sleep();
            assertGreaterZero(count.get());
            assertZero(sameGroupCount.get());
            assertGreaterZero(differentGroupCount.get());
            assertGreaterZero(afterCount.get());
            assertGreaterZero(afterCount2.get());
        }

        @Test
        @Order(11)
        void executeGroup() {
            eventScheduler.disable(job.getGroup());
            delay();
            reset();
            eventScheduler.execute(job.getGroup());
            sleep();
            assertGreaterZero(count.get());
            assertGreaterZero(sameGroupCount.get());
            assertGreaterZero(differentGroupCount.get());
            assertGreaterZero(afterCount.get());
            assertGreaterZero(afterCount2.get());
        }
    }

    @DisplayName("Cluster")
    @SpringBootTest(
            properties = {"spring.profiles.active=cluster"}
    )
    @Sql(scripts = {"/schema/cluster.sql"})
    @Nested
    @TestInstance(PER_CLASS)
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    @Import(EventJobMessageServiceImpl.class)
    class Cluster {
        //        private final String tag = String.valueOf(System.currentTimeMillis());
        private final String tag = "job";
        private final String afterTag = "after-" + System.currentTimeMillis();
        private final String afterTag2 = "after2-" + System.currentTimeMillis();
        private final Map<String, Object> map = new HashMap<>();

        private final EventJob job = EventJob.builder()
                .group(tag)
                .name(tag)
                .event(tag)
                .cron("*/1 * * * * ?")
                .timezone("+00:00")
                .data(map)
                .build();
        private final EventJob afterJob = EventJob.builder()
                .group(afterTag)
                .name(afterTag)
                .event(afterTag)
                .afterGroup(tag)
                .afterName(tag)
                .timezone("+00:00")
                .data(map)
                .build();
        private final EventJob afterJob2 = EventJob.builder()
                .group(afterTag2)
                .name(afterTag2)
                .event(afterTag2)
                .afterGroup(afterTag)
                .afterName(afterTag)
                .timezone("+00:00")
                .data(map)
                .build();
        private final EventJob sameGroupJob = EventJob.builder()
                .group(tag)
                .name(tag + "2")
                .event(tag + "2")
                .cron("*/1 * * * * ?")
                .timezone("+08:00")
                .data(map)
                .build();
        private final EventJob differentGroupJob = EventJob.builder()
                .group(tag + "3")
                .name(tag)
                .event(tag + "3")
                .cron("*/1 * * * * ?")
                .timezone("+00:00")
                .data(map)
                .build();
        @Autowired
        private JdbcTemplate jdbcTemplate;
        @Autowired
        private EventScheduler eventScheduler;

        {
            map.put("test", "test");
        }

        private void delay() {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void sleep() {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @BeforeAll
        void beforeAll() throws SQLException {
            DataSourceSchemaUtil.init(Objects.requireNonNull(jdbcTemplate.getDataSource()), "schema/cluster.sql");
            eventScheduler.add(job);
            eventScheduler.add(afterJob);
            eventScheduler.add(afterJob2);
            eventScheduler.add(sameGroupJob);
            eventScheduler.add(differentGroupJob);
            for (int i = 0; i < 3; i++) {
                ApplicationTest2.main(new String[]{"--spring.profiles.active=cluster", "--server.port=" + (9091 + i)});
            }
        }

        @BeforeEach
        void before() {
            eventScheduler.add(job);
            eventScheduler.add(afterJob);
            eventScheduler.add(afterJob2);
            eventScheduler.add(sameGroupJob);
            eventScheduler.add(differentGroupJob);
        }

        @AfterAll
        void after() {
            eventScheduler.remove(job.getGroup());
            eventScheduler.remove(afterJob.getGroup());
            eventScheduler.remove(afterJob2.getGroup());
            eventScheduler.remove(sameGroupJob.getGroup());
            eventScheduler.remove(differentGroupJob.getGroup());
        }

        private void reset() {
            jdbcTemplate.update("update log set `count` = 0");
        }

        private Integer getSumCount(EventJob job) {
            return jdbcTemplate.queryForObject("select ifnull(sum(`count`),0) from log where instance = ? and `group` = ? and name = ? "
                    , Integer.class
                    , eventScheduler.getInstance()
                    , job.getGroup()
                    , job.getName()
            );
        }

        private void check() {
            Long count = jdbcTemplate.queryForObject("select count(*) from (select count(*) from log where count>0 group by instance,`group`,name)t", Long.class);
            if (count == null) {
                fail("check count: null");
            } else {
                assertTrue(count > 0, () -> String.format("check count: %d", count));
            }
        }

        @Test
        @Order(3)
        void disable() {
            eventScheduler.disable(job.getGroup(), job.getName());
            delay();
            reset();
            sleep();
            assertZero(getSumCount(job));
            assertGreaterZero(getSumCount(sameGroupJob));
            assertGreaterZero(getSumCount(differentGroupJob));
            assertZero(getSumCount(afterJob));
            assertZero(getSumCount(afterJob2));
        }

        @Test
        @Order(4)
        void enable() {
            eventScheduler.enable(job.getGroup(), job.getName());
            reset();
            sleep();
            assertGreaterZero(getSumCount(job));
            assertGreaterZero(getSumCount(sameGroupJob));
            assertGreaterZero(getSumCount(differentGroupJob));
            assertGreaterZero(getSumCount(afterJob));
            assertGreaterZero(getSumCount(afterJob2));
        }


        @Test
        @Order(5)
        void disableGroup() {
            eventScheduler.disable(job.getGroup());
            delay();
            reset();
            sleep();
            assertZero(getSumCount(job));
            assertZero(getSumCount(sameGroupJob));
            assertGreaterZero(getSumCount(differentGroupJob));
            assertZero(getSumCount(afterJob));
            assertZero(getSumCount(afterJob2));
        }

        @Test
        @Order(6)
        void enableGroup() {
            eventScheduler.enable(job.getGroup());
            reset();
            sleep();
            assertGreaterZero(getSumCount(job));
            assertGreaterZero(getSumCount(sameGroupJob));
            assertGreaterZero(getSumCount(differentGroupJob));
            assertGreaterZero(getSumCount(afterJob));
            assertGreaterZero(getSumCount(afterJob2));
        }

        @Test
        @Order(8)
        void remove() {
            eventScheduler.remove(job.getGroup(), job.getName());
            delay();
            reset();
            sleep();
            assertZero(getSumCount(job));
            assertGreaterZero(getSumCount(sameGroupJob));
            assertGreaterZero(getSumCount(differentGroupJob));
            assertZero(getSumCount(afterJob));
            assertZero(getSumCount(afterJob2));
        }

        @Test
        @Order(9)
        void removeGroup() {
            eventScheduler.remove(job.getGroup());
            delay();
            reset();
            sleep();
            assertZero(getSumCount(job));
            assertZero(getSumCount(sameGroupJob));
            assertGreaterZero(getSumCount(differentGroupJob));
            assertZero(getSumCount(afterJob));
            assertZero(getSumCount(afterJob2));
        }

        @Test
        @Order(10)
        void execute() {
            eventScheduler.disable(job.getGroup());
            delay();
            reset();
            eventScheduler.execute(job.getGroup(), job.getName());
            sleep();
            assertGreaterZero(getSumCount(job));
            assertZero(getSumCount(sameGroupJob));
            assertGreaterZero(getSumCount(differentGroupJob));
            assertGreaterZero(getSumCount(afterJob));
            assertGreaterZero(getSumCount(afterJob2));
        }

        @Test
        @Order(11)
        void executeGroup() {
            eventScheduler.disable(job.getGroup());
            delay();
            reset();
            eventScheduler.execute(job.getGroup());
            sleep();
            assertGreaterZero(getSumCount(job));
            assertGreaterZero(getSumCount(sameGroupJob));
            assertGreaterZero(getSumCount(differentGroupJob));
            assertGreaterZero(getSumCount(afterJob));
            assertGreaterZero(getSumCount(afterJob2));
            check();
        }
    }

}

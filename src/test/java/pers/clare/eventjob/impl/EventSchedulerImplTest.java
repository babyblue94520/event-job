package pers.clare.eventjob.impl;

import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import pers.clare.eventjob.EventScheduler;
import pers.clare.eventjob.function.JobHandler;
import pers.clare.eventjob.vo.EventJob;
import pers.clare.h2.H2Application;
import pers.clare.test.ApplicationTest2;
import pers.clare.test.eventjob.EventJobMessageServiceImpl;
import pers.clare.test.eventjob.EventJobRegister;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

    private void assertRange(Integer min, Integer max, Integer count) {
        assertTrue(count > min && count < max, () -> String.format("count: %d", count));
    }

    static {
        H2Application.main(null);
    }

    @DisplayName("Single")
    @ActiveProfiles("single")
    @SpringBootTest
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

        @Test
        @Order(13)
        void longHandler() throws InterruptedException {
            AtomicInteger count = new AtomicInteger();
            EventJob job = EventJob.builder()
                    .group("test")
                    .name("test")
                    .event("test")
                    .cron("* * * * * ?")
                    .timezone("+00:00")
                    .build();
            eventScheduler.addHandler(job.getEvent(), (eventJob) -> {
                count.incrementAndGet();
            });
            eventScheduler.addHandler(job.getEvent(), (eventJob) -> {
                Thread.sleep(3000);
            });
            eventScheduler.add(job);
            Thread.sleep(10000);
            System.out.println(count.get());
        }

        @Test
        @Order(12)
        void abortOnError() throws InterruptedException {
            AtomicInteger count = new AtomicInteger();
            AtomicInteger count2 = new AtomicInteger();
            AtomicInteger count3 = new AtomicInteger();
            int target = 5;
            int target2 = 3;
            eventScheduler.addHandler(job.getEvent(), (eventJob) -> {
                if (count.incrementAndGet() == target) {
                    throw new RuntimeException();
                }
            });

            eventScheduler.addHandler(afterJob.getEvent(), (eventJob) -> {
                if (count2.incrementAndGet() == target) {
                    throw new RuntimeException();
                }
            });

            eventScheduler.addHandler(afterJob2.getEvent(), (eventJob) -> {
                if (count3.incrementAndGet() == target2) {
                    throw new RuntimeException();
                }
            });
            Thread.sleep(10000);
            assertEquals(target, count.get());
            assertEquals(target, count2.get());
            assertEquals(target2, count3.get());

        }

        @Test
        @Order(13)
        void updateJob() throws InterruptedException {
            EventJob job = EventJob.builder()
                    .group("test")
                    .name("test")
                    .event("test")
                    .cron("*/3 * * * * ?")
                    .timezone("+00:00")
                    .build();
            AtomicInteger count = new AtomicInteger();
            eventScheduler.addHandler(job.getEvent(), (eventJob) -> {
                count.incrementAndGet();
            });
            eventScheduler.add(job);
            int target = 5;

            updateTest(target, count, () -> {
                eventScheduler.disable(job.getGroup(), job.getName());
            }, () -> {
                eventScheduler.enable(job.getGroup(), job.getName());
            });
            updateTest(target, count, () -> {
                eventScheduler.disable(job.getGroup());
            }, () -> {
                eventScheduler.enable(job.getGroup());
            });


            updateTest(target, count, () -> {
                eventScheduler.remove(job.getGroup());
            }, () -> {
                eventScheduler.add(job);
            });
            updateTest(target, count, () -> {
                eventScheduler.remove(job.getGroup(), job.getName());
            }, () -> {
                eventScheduler.add(job);
            });
        }

        void updateTest(int target, AtomicInteger count, Runnable before, Runnable after) throws InterruptedException {
            int next = 0;
            while (next < target) {
                int c = count.get();
                if (c > next) {
                    next = c;
                    before.run();
                    Thread.sleep(1000);
                    after.run();
                } else {
                    Thread.sleep(500);
                }
            }
            assertEquals(target, next);
        }
    }

    @DisplayName("Cluster")
    @ActiveProfiles("cluster")
    @SpringBootTest
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
                .cron("* * * * * ?")
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
                .cron("* * * * * ?")
                .timezone("+08:00")
                .data(map)
                .build();
        private final EventJob differentGroupJob = EventJob.builder()
                .group(tag + "3")
                .name(tag)
                .event(tag + "3")
                .cron("* * * * * ?")
                .timezone("+00:00")
                .data(map)
                .build();

        @Autowired
        private EventScheduler eventScheduler;

        {
            map.put("test", "test");
        }

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

        @BeforeAll
        void beforeAll() {
            eventScheduler.add(job);
            eventScheduler.add(afterJob);
            eventScheduler.add(afterJob2);
            eventScheduler.add(sameGroupJob);
            eventScheduler.add(differentGroupJob);
            for (int i = 0; i < 3; i++) {
                ApplicationTest2.main(new String[]{"--spring.profiles.active=cluster", "--server.port=0"});
            }
        }

        @BeforeEach
        void before() {
            eventScheduler.add(job);
            eventScheduler.add(afterJob);
            eventScheduler.add(afterJob2);
            eventScheduler.add(sameGroupJob);
            eventScheduler.add(differentGroupJob);
            reset();
        }

        @AfterEach
        void after() {
            eventScheduler.remove(job.getGroup());
            eventScheduler.remove(afterJob.getGroup());
            eventScheduler.remove(afterJob2.getGroup());
            eventScheduler.remove(sameGroupJob.getGroup());
            eventScheduler.remove(differentGroupJob.getGroup());
            sleep();
        }

        private void reset() {
            EventJobRegister.reset();
        }

        private Integer getSumCount(EventJob eventJob) {
            return EventJobRegister.getCount(eventJob);
        }

        @Test
        @Order(2)
        void count() throws InterruptedException {
            int target = 10;
            Thread.sleep(target * 1000);
            int min = target - 2;
            int max = target + 2;
            assertRange(min, max, getSumCount(job));
            assertRange(min, max, getSumCount(sameGroupJob));
            assertRange(min, max, getSumCount(differentGroupJob));
            assertRange(min, max, getSumCount(afterJob));
            assertRange(min, max, getSumCount(afterJob2));
        }

        @Test
        @Order(3)
        void disable() {
            doDisable();
            doEnable();
        }

        void doDisable() {
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
            doEnable();
            doDisable();
            doEnable();
        }

        void doEnable() {
            eventScheduler.enable(job.getGroup(), job.getName());
            reset();
            sleep();
            assertGreaterZero(getSumCount(job));
            ;
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
            eventScheduler.disable(job.getGroup());
            delay();
            reset();
            eventScheduler.enable(job.getGroup());
            sleep();
            eventScheduler.disable(job.getGroup());
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
        }


    }


}

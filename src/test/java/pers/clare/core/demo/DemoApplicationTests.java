package pers.clare.core.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pers.clare.core.scheduler.Job;
import pers.clare.core.scheduler.Scheduler;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class DemoApplicationTests {
    static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    @Autowired
    Scheduler scheduler;

    private long interval = 3000;

    @Test
    void contextLoads() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            Job job = new Job("Test", "test" + i, "test" + i, "+00:00", "0/1 * * * * ?", true, new HashMap<>());
            update(job);
        }
        Thread.sleep(300000);
    }

    private void update(Job job) {
        executor.schedule(() -> {
            job.getData().put("test", System.currentTimeMillis());
            scheduler.add(job);
            System.out.println("update "+job.getName());
            System.out.println(job.getData());
            disable(job);
        }, interval, TimeUnit.MILLISECONDS);
    }

    private void disable(Job job) {
        executor.schedule(() -> {
            scheduler.disable(job.getGroup(), job.getName());
            System.out.println("disable " + job.getName());
            enable(job);
        }, interval, TimeUnit.MILLISECONDS);
    }

    private void enable(Job job) {
        executor.schedule(() -> {
            scheduler.enable(job.getGroup(), job.getName());
            System.out.println("enable " + job.getName());
            remove(job);
        }, interval, TimeUnit.MILLISECONDS);
    }

    private void remove(Job job) {
        executor.schedule(() -> {
            try{
                scheduler.remove(job.getGroup(), job.getName());
                System.out.println("remove " + job.getName());
            }catch (Exception e){
                e.printStackTrace();
            }
            add(job);
        }, interval, TimeUnit.MILLISECONDS);
    }

    private void add(Job job) {
        executor.schedule(() -> {
            scheduler.add(job);
            System.out.println("add " + job.getName());
            update(job);
        }, interval, TimeUnit.MILLISECONDS);
    }
}

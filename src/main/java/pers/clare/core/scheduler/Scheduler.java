package pers.clare.core.scheduler;

import org.springframework.boot.CommandLineRunner;
import java.util.function.Consumer;

public interface Scheduler extends CommandLineRunner {

    public void handler(String group, String name, Consumer<EventJob> consumer);

    public void add(Job job) throws RuntimeException;

    public void remove(String group, String name) throws RuntimeException;

    public void enable(String group, String name) throws RuntimeException;

    public void disable(String group, String name) throws RuntimeException;

}


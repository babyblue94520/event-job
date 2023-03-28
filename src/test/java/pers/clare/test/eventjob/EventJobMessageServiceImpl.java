package pers.clare.test.eventjob;

import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import pers.clare.eventjob.EventJobMessageService;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Log4j2
@Service
public class EventJobMessageServiceImpl implements EventJobMessageService {

    private static final List<Consumer<String>> listeners = new CopyOnWriteArrayList<>();
    private static final ExecutorService executor = Executors.newFixedThreadPool(1);


    @Override
    public void send(String body) {
        executor.submit(() -> {
            listeners.forEach(consumer -> consumer.accept(body));
        });
    }

    @Override
    public void addListener(Consumer<String> listener) {
        listeners.add(listener);
    }
}

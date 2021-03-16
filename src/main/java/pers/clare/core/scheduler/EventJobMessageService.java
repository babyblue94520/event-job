package pers.clare.core.scheduler;


import java.util.function.Consumer;

public interface EventJobMessageService {

    void onConnected(Runnable runnable);

    void send(String topic, String body);

    void addListener(String topic, Consumer<String> listener);

}

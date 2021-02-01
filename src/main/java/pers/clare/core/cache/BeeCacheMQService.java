package pers.clare.core.cache;


import java.util.function.Consumer;

public interface BeeCacheMQService {

    void onConnected(Runnable runnable);

    void send(String topic, String body);

    void addListener(String topic, Consumer<String> listener);

}

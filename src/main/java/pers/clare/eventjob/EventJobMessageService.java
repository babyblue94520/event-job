package pers.clare.eventjob;

import org.springframework.lang.NonNull;

import java.util.function.Consumer;

@SuppressWarnings("UnusedReturnValue")
public interface EventJobMessageService {

    Runnable onConnected(@NonNull Runnable runnable);

    String send(@NonNull String topic, @NonNull String body);

    Consumer<String> addListener(@NonNull String topic, @NonNull Consumer<String> listener);

}

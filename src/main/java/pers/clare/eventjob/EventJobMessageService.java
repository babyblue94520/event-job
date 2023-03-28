package pers.clare.eventjob;

import org.springframework.lang.NonNull;

import java.util.function.Consumer;

@SuppressWarnings("UnusedReturnValue")
public interface EventJobMessageService {

    void send(@NonNull String body);

    void addListener(@NonNull Consumer<String> listener);

}

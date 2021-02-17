package pers.clare.core.scheduler.exception;

public class JobException extends RuntimeException {
    public JobException(String message) {
        super(message);
    }

    public JobException(Throwable cause) {
        super(cause);
    }
}

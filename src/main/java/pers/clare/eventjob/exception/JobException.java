package pers.clare.eventjob.exception;

public class JobException extends RuntimeException {
    public JobException(String message) {
        super(message);
    }

    public JobException(Throwable cause) {
        super(cause);
    }
}

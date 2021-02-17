package pers.clare.core.scheduler.exception;

public class JobNotExistException extends RuntimeException {
    public JobNotExistException(String message) {
        super(message);
    }
}

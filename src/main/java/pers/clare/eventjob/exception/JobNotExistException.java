package pers.clare.eventjob.exception;

public class JobNotExistException extends RuntimeException {
    public JobNotExistException(String message) {
        super(message);
    }
}

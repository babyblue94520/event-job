package pers.clare.eventjob.exception;

@SuppressWarnings("unused")
public class JobNotExistException extends RuntimeException {
    public JobNotExistException(String message) {
        super(message);
    }
}

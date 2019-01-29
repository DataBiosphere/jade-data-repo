package bio.terra.service;

public class AsyncException extends RuntimeException {


    public AsyncException(String message) {
        super(message);
    }

    public AsyncException(String message, Throwable throwable) {
        super(message, throwable);
    }

}

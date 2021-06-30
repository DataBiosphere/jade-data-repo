package bio.terra.app.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.stereotype.Component;

@Component
class RetryListener extends RetryListenerSupport {
    private final Logger logger = LoggerFactory
        .getLogger(RetryListener.class);

    @Override
    public <T, E extends Throwable> void close(RetryContext context,
                                               RetryCallback<T, E> callback, Throwable throwable) {

        logger.info("closing retry");
        super.close(context, callback, throwable);
    }

    @Override
    public <T, E extends Throwable> void onError(RetryContext context,
                                                 RetryCallback<T, E> callback,
                                                 Throwable throwable) {
        logger.error("Exception Occurred, Retry Count {} ", context.getRetryCount());
        super.onError(context, callback, throwable);
    }

    @Override
    public <T, E extends Throwable> boolean open(RetryContext context,
                                                 RetryCallback<T, E> callback) {
        logger.info("Open retryable.");

        return super.open(context, callback);
    }
}

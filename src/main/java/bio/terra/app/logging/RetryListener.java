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

        // Only log if retries have been exhausted
        if (throwable != null) {
            logger.error("[Retries Exhausted] Context: {}, Throwable: ", context, throwable);
        } else {
            logger.debug("Closing debugger after successful retry");
        }
        super.close(context, callback, throwable);
    }

    @Override
    public <T, E extends Throwable> void onError(RetryContext context,
                                                 RetryCallback<T, E> callback,
                                                 Throwable throwable) {
        logger.warn("[Retryable Exception] Retry Count {}, Context: {}, Throwable: ",
            context.getRetryCount(), context, throwable);
        super.onError(context, callback, throwable);
    }

    @Override
    public <T, E extends Throwable> boolean open(RetryContext context,
                                                 RetryCallback<T, E> callback) {
        logger.debug("Hit Retryable Exception - opening retry function.");
        return super.open(context, callback);
    }
}

package bio.terra.filesystem;

import bio.terra.filesystem.exception.FileSystemExecutionException;
import com.google.api.core.ApiFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
public class FireStoreUtils {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreUtils");

/*
    private Firestore firestore;

    @Autowired
    public FireStoreUtils(Firestore firestore) {
        this.firestore = firestore;
    }
*/

    public <T> T transactionGet(String op, ApiFuture<T> transaction) {
        try {
            return transaction.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException(op + " - execution interrupted", ex);
        } catch (ExecutionException ex) {
            // If this exception has a cause that is a runtime exception, then we rethrow the cause.
            // Typically, the cause is one of our file system messages we want to see.
            // If there is no cause or it is not runtime, we wrap the exception in a generic one of ours.
            Throwable throwable = ex.getCause();
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException)throwable;
            }
            throw new FileSystemExecutionException(op + " - execution exception", ex);
        }
    }

}

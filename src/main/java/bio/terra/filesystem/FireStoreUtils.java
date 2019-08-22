package bio.terra.filesystem;

import bio.terra.filesystem.exception.FileSystemExecutionException;
import com.google.api.core.ApiFuture;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
public class FireStoreUtils {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreUtils");

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

    public String getObjectName(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        if (pathParts.length == 0) {
            return StringUtils.EMPTY;
        }
        return pathParts[pathParts.length - 1];
    }

    public String getDirectoryPath(String path) {
        String[] pathParts = StringUtils.split(path, '/');
        if (pathParts.length <= 1) {
            // We are at the root; no containing directory
            return StringUtils.EMPTY;
        }
        int endIndex = pathParts.length - 1;
        return '/' + StringUtils.join(pathParts, '/', 0, endIndex);
    }

    public String getFullPath(String dirPath, String name) {
        // Originally, this was a method in FireStoreObject, but the Firestore client complained about it,
        // because it was not a set/get for an actual class member. Very picky, that!
        // There are three cases here:
        // - the path and name are empty: that is the root. Full path is "/"
        // - the path is "/" and the name is not empty: dir in the root. Full path is "/name"
        // - the path is "/name" and the name is not empty: Full path is path + "/" + name
        String path = StringUtils.EMPTY;
        if (StringUtils.isNotEmpty(dirPath) && !StringUtils.equals(dirPath, "/")) {
            path = dirPath;
        }
        return path + '/' + name;
    }

}

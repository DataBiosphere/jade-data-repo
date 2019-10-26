package bio.terra.service.filedata.google.firestore;

import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.filedata.exception.FileSystemRetryException;
import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.AbortedException;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreException;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.PureJavaCrc32C;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

@Component
public class FireStoreUtils {
    private final Logger logger = LoggerFactory.getLogger(FireStoreUtils.class);

    public <T> T transactionGet(String op, ApiFuture<T> transaction) {
        try {
            return transaction.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException(op + " - execution interrupted", ex);
        } catch (ExecutionException ex) {
            // The ExecutionException wraps the underlying exception caught in the Future, so we need
            // to examine the properties of the cause to understand what to do.
            // Possible outcomes:
            // - throw FileSystemRetryException for retryable firestore exceptions to ask the step to retry
            // - throw FileSystemExecutionException for other firestore exceptions (not retryable)
            // - rethrow RuntimeExceptions to expose other unexpected exceptions
            // - throw FileSystemExecutionException to wrap non-Runtime (oddball) exceptions

            Throwable throwable = ex.getCause();
            while (throwable instanceof ExecutionException) {
                ExecutionException eex = (ExecutionException)throwable;
                throwable = eex.getCause();
            }
            if (throwable instanceof AbortedException) {
                AbortedException aex = (AbortedException)throwable;
                // TODO: in general, log + rethrow is bad form. For now, I want to make sure we see these in
                //  the log as they happen. Once we are comfortable that retry is working properly, we can
                //  rely on the Stairway debug logging as needed.
                String msg = "Retrying aborted exception: " + aex.toString();
                logger.info(msg);
                throw new FileSystemRetryException(msg);
            }
            if (throwable instanceof FirestoreException) {
                FirestoreException fex = (FirestoreException)throwable;
                String msg = "Retrying firestore exception: " + fex.toString();
                logger.info(msg);
                throw new FileSystemRetryException(msg);
            }
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException)throwable;
            }
            throw new FileSystemExecutionException(op + " - execution exception wrapping: " + throwable.toString());
        }
    }

    public String getName(String path) {
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
        // Originally, this was a method in FireStoreDirectoryEntry, but the Firestore client complained about it,
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

    /**
     * This code is a bit ugly, but here is why...
     * (from https://cloud.google.com/firestore/docs/solutions/delete-collections)
     * <ul>
     * <li>There is no operation that atomically deletes a collection.</li>
     * <li>Deleting a document does not delete the documents in its subcollections.</li>
     * <li>If your documents have dynamic subcollections, (we don't do this!)
     *     it can be hard to know what data to delete for a given path.</li>
     * <li>Deleting a collection of more than 500 documents requires multiple batched
     *     write operations or hundreds of single deletes.</li>
     * </ul>
     *
     * Our objects are small, so I think we can use the maximum batch size without
     * concern for using too much memory.
     */
    void scanCollectionObjects(Firestore firestore,
                               String collectionId,
                               int batchSize,
                               Consumer<QueryDocumentSnapshot> func) {
        CollectionReference datasetCollection = firestore.collection(collectionId);
        try {
            int batchCount = 0;
            int visited;
            do {
                visited = 0;
                ApiFuture<QuerySnapshot> future = datasetCollection.limit(batchSize).get();
                List<QueryDocumentSnapshot> documents = future.get().getDocuments();
                batchCount++;
                logger.info("Visiting batch " + batchCount + " of ~" + batchSize + " documents");
                for (QueryDocumentSnapshot document : documents) {
                    func.accept(document);
                    visited++;
                }
            } while (visited >= batchSize);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new FileSystemExecutionException("scanning collection - execution interrupted", ex);
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("scanning collection - execution exception", ex);
        }
    }

    String computeMd5(String input) {
        return StringUtils.lowerCase(DigestUtils.md5Hex(input));
    }

    String computeCrc32c(String input) {
        byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
        PureJavaCrc32C crc = new PureJavaCrc32C();
        crc.update(inputBytes, 0, inputBytes.length);
        return Long.toHexString(crc.getValue());
    }

}

package bio.terra.service.filedata.google.firestore;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import com.google.api.core.ApiFuture;
import com.google.api.gax.rpc.AbortedException;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.api.gax.rpc.InternalException;
import com.google.api.gax.rpc.UnavailableException;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreException;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.Transaction;
import com.google.cloud.storage.StorageException;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FireStoreUtils {

  private final Logger logger = LoggerFactory.getLogger(FireStoreUtils.class);
  private static final int SLEEP_BASE_SECONDS = 1;
  private static final int SLEEP_MAX_SECONDS = 20;
  // Firestore limits batches to 500
  public static final int MAX_FIRESTORE_BATCH_SIZE = 500;

  private ConfigurationService configurationService;

  @Autowired
  public FireStoreUtils(ConfigurationService configurationService) {
    this.configurationService = configurationService;
  }

  public int getFirestoreRetries() {
    try {
      return configurationService.getParameterValue(ConfigEnum.FIRESTORE_RETRIES);
    } catch (Exception ex) {
      logger.info("No value set for FIRESTORE_RETRIES parameter. Defaulting to 1.");
      return 1;
    }
  }

  <T> T transactionGet(String op, ApiFuture<T> transaction) throws InterruptedException {
    try {
      return transaction.get();
    } catch (ExecutionException ex) {
      throw handleExecutionException(ex, op);
    }
  }

  RuntimeException handleExecutionException(Throwable ex, String op) {
    // The ExecutionException wraps the underlying exception caught in the FireStore Future, so we
    // need
    // to examine the properties of the cause to understand what to do.
    // Possible outcomes:
    // - FileSystemAbortTransactionException for retryable firestore exceptions to ask the step to
    // retry
    // - FileSystemExecutionException for other firestore exceptions
    // - RuntimeExceptions to expose other unexpected exceptions
    // - FileSystemExecutionException to wrap non-Runtime (oddball) exceptions
    Throwable throwable = ex;
    while (throwable instanceof ExecutionException) {
      throwable = throwable.getCause();
    }
    if (throwable instanceof AbortedException) {
      AbortedException aex = (AbortedException) throwable;
      // TODO: in general, log + rethrow is bad form. For now, I want to make sure we see these in
      //  the log as they happen. Once we are comfortable that retry is working properly, we can
      //  rely on the Stairway debug logging as needed.
      String msg =
          "["
              + op
              + "] FirestoreUtils.handleExecutionException - Retrying aborted exception: "
              + aex;
      logger.info(msg);
      return new FileSystemAbortTransactionException(msg, aex);
    }
    if (throwable instanceof FirestoreException) {
      FirestoreException fex = (FirestoreException) throwable;
      String msg =
          "["
              + op
              + "] FirestoreUtils.handleExecutionException - Retrying firestore exception: "
              + fex;
      logger.info(msg);
      return new FileSystemAbortTransactionException(msg, fex);
    }
    if (throwable instanceof RuntimeException) {
      logger.info("[{}] FirestoreUtils.handleExecutionException - RuntimeException", op);
      return (RuntimeException) throwable;
    }
    logger.info(
        "[{}] FirestoreUtils.handleExecutionException - Wrapping w/ FileSystemExecutionException",
        op);
    return new FileSystemExecutionException(
        op + " - execution exception wrapping: " + throwable, throwable);
  }

  /**
   * This code is a bit ugly, but here is why... (from
   * https://cloud.google.com/firestore/docs/solutions/delete-collections)
   *
   * <ul>
   *   <li>There is no operation that atomically deletes a collection.
   *   <li>Deleting a document does not delete the documents in its subcollections.
   *   <li>If your documents have dynamic subcollections, (we don't do this!) it can be hard to know
   *       what data to delete for a given path.
   *   <li>Deleting a collection of more than 500 documents requires multiple batched write
   *       operations or hundreds of single deletes.
   * </ul>
   *
   * <p>Our objects are small, so I think we can use the maximum batch size without concern for
   * using too much memory.
   */
  <V> void scanCollectionObjects(
      Firestore firestore,
      String collectionId,
      int batchSize,
      ApiFutureGenerator<V, QueryDocumentSnapshot> generator)
      throws InterruptedException {
    CollectionReference datasetCollection = firestore.collection(collectionId);
    int batchCount = 0;
    List<QueryDocumentSnapshot> documents;
    do {
      documents =
          runTransactionWithRetry(
              firestore,
              xn -> {
                Query query = datasetCollection.limit(batchSize);
                ApiFuture<QuerySnapshot> querySnapshot = xn.get(query);
                return querySnapshot.get().getDocuments();
              },
              "scanCollectionObjects",
              " scanning " + batchSize + " items for collection id: " + collectionId);
      batchCount++;
      if (!documents.isEmpty()) {
        logger.info("Visiting batch " + batchCount + " of ~" + batchSize + " documents");
      }
      batchOperation(documents, generator);

    } while (documents.size() > 0);
  }

  /**
   * Perform the specified Firestore operation against a specified list of inputs in batch.
   *
   * @param inputs A list containing the inputs to the function to be applied in batch. Note: it's a
   *     bad idea to pass futures in as the input and just have generator be an identity function
   *     since the future does not re-resolve after an initial execution
   * @param generator A generator that provides a future given an input from the inputs parameter
   * @param <T> The class of the objects in the input list
   * @param <V> The class of the objects that will result when the generated futures resolve
   * @return A list of resolved futures resulting in having run the specified operation in batch.
   *     Note: the order of the list matches with the order of the input list objects
   * @throws InterruptedException If a call to Firestore is interrupted
   */
  <T, V> List<T> batchOperation(List<V> inputs, ApiFutureGenerator<T, V> generator)
      throws InterruptedException {
    int inputSize = inputs.size();
    // We drive the retry processing by which outputs have not been filled in,
    // so we initialize the outputs to be all null -> not filled in.
    List<T> outputs = new ArrayList<>(inputSize);
    for (int i = 0; i < inputSize; i++) {
      outputs.add(null);
    }

    int noProgressCount = 0;
    while (true) {
      List<ApiFuture<T>> futures = new ArrayList<>(inputSize);

      // generate a request for every not completed output
      int requestCount = 0;
      for (int i = 0; i < inputSize; i++) {
        if (outputs.get(i) != null) {
          futures.add(null);
        } else {
          futures.add(generator.accept(inputs.get(i)));
          requestCount++;
        }
      }
      logger.info(
          "[batchOperation] Processing {} requests, input size: {}", requestCount, inputSize);
      if (requestCount == 0) {
        break;
      }

      // try to collect a response for every request we generated
      int completeCount = 0;
      for (int i = 0; i < inputSize; i++) {
        ApiFuture<T> future = futures.get(i);
        if (future != null) {
          try {
            outputs.set(i, future.get());
            completeCount++;
          } catch (DeadlineExceededException
              | UnavailableException
              | AbortedException
              | InternalException
              | StatusRuntimeException
              | ExecutionException ex) {
            if (shouldRetry(ex, true)) {
              logger.warn(
                  "[batchOperation] Retry-able error in firestore future get - input: "
                      + inputs.get(i)
                      + "; output: "
                      + outputs.get(i)
                      + "; message: "
                      + ex.getMessage(),
                  ex);
            } else {
              throw new FileSystemExecutionException(
                  "[batchOperation] Parent exception caught but neither parent nor nested "
                      + "exceptions were designated for retry.",
                  ex);
            }
          }
        }
      }

      // If we completed our requests we are done
      if (completeCount == requestCount) {
        break;
      }

      final long retryWait =
          (long) Math.min(SLEEP_MAX_SECONDS, (SLEEP_BASE_SECONDS * Math.pow(2.5, noProgressCount)));
      if (completeCount == 0) {
        noProgressCount++;
        if (noProgressCount > getFirestoreRetries()) {
          throw new FileSystemExecutionException(
              "[batchOperation] Operation failed - "
                  + getFirestoreRetries()
                  + " tries with no progress.");
        }
      }
      // Exponential backoff
      logger.info(
          "[batchOperation] will attempt retry #{} after {} millisecond pause. {} requests completed this round.",
          noProgressCount,
          retryWait,
          completeCount);
      TimeUnit.SECONDS.sleep(retryWait);
    }

    return outputs;
  }

  // For batch operations, we want to also include the AbortedException as retryable
  public static boolean shouldRetry(Throwable throwable, boolean isBatch) {
    if (throwable == null) {
      return false; // Did not find a retry-able exception
    }
    if (throwable instanceof DeadlineExceededException
        || throwable instanceof UnavailableException
        || throwable instanceof InternalException
        || throwable instanceof StatusRuntimeException
        || throwable instanceof GoogleResourceException
        || throwable instanceof StorageException
        || (isBatch && throwable instanceof AbortedException)) {
      return true;
    }
    return shouldRetry(throwable.getCause(), isBatch);
  }

  public <T> T runTransactionWithRetry(
      Firestore firestore,
      Transaction.Function<T> firestoreFunction,
      String transactionOp,
      String warnMessage)
      throws InterruptedException {
    int retry = 0;
    while (true) {
      try {
        ApiFuture<T> transaction = firestore.runTransaction(firestoreFunction);

        return transactionGet(transactionOp, transaction);
      } catch (Exception ex) {
        final long retryWait =
            Math.min(SLEEP_MAX_SECONDS, (long) (SLEEP_BASE_SECONDS * Math.pow(2.5, retry)));
        if (retry < getFirestoreRetries() && FireStoreUtils.shouldRetry(ex, false)) {
          // perform retry
          retry++;
          logger.warn(
              "[transaction retry] Retry-able error in firestore transactions - {} "
                  + "- will attempt retry #{}"
                  + " after {} second pause. Message: {}",
              warnMessage,
              retry,
              retryWait,
              ex.getMessage());
          TimeUnit.SECONDS.sleep(retryWait);
        } else {
          if (retry > getFirestoreRetries()) {
            logger.error("[transaction retry] Ran out of retries - {}", warnMessage);
          }
          throw ex;
        }
      }
    }
  }

  public boolean collectionHasDocuments(Firestore firestore, Query collectionQuery)
      throws InterruptedException {
    int docCount =
        runTransactionWithRetry(
            firestore,
            (xn -> {
              Query limitedQuery = collectionQuery.limit(1);
              ApiFuture<QuerySnapshot> querySnapshot = xn.get(limitedQuery);
              return querySnapshot.get().getDocuments().size();
            }),
            "collectionHasDocuments",
            "Querying firestore and checking if collection is empty");
    return docCount > 0;
  }
}

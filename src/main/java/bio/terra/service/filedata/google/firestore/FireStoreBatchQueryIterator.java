package bio.terra.service.filedata.google.firestore;

import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FireStoreBatchQueryIterator {
  private static final Logger logger = LoggerFactory.getLogger(FireStoreBatchQueryIterator.class);

  private static final int RETRIES = 3;
  private static final int SLEEP_SECONDS = 1;

  private final Query baseQuery;
  private final int batchSize;
  private List<QueryDocumentSnapshot> currentList;
  private int count;

  /**
   * Construct and iterator over a query with a specific batch size.
   *
   * @param baseQuery the base query
   * @param batchSize the size of batches to request
   */
  public FireStoreBatchQueryIterator(Query baseQuery, int batchSize) {
    this.baseQuery = baseQuery;
    this.batchSize = batchSize;
    this.currentList = null;
    this.count = 0;
  }

  /**
   * Get the next batch of documents
   *
   * @return a list of documents, or no if the iteration is complete
   * @throws InterruptedException on pod shut down
   */
  public List<QueryDocumentSnapshot> getBatch() throws InterruptedException {
    Query query;
    if (currentList == null) {
      // First time through we start at the beginning
      query = baseQuery.limit(batchSize);
    } else {
      int listSize = currentList.size();
      if (listSize < batchSize) {
        // We delivered our last list on the previous call
        return null;
      }
      QueryDocumentSnapshot lastDoc = currentList.get(listSize - 1);
      query = baseQuery.startAfter(lastDoc).limit(batchSize);
    }

    // Get the next batch
    logger.info("Retrieving batch {} with batch size of {}", count, batchSize);
    count++;

    for (int i = 0; i < RETRIES; i++) {
      ApiFuture<QuerySnapshot> querySnapshot = query.get();

      try {
        currentList = querySnapshot.get().getDocuments();
        if (currentList.size() == 0) {
          // Nothing to return so we at the end of the iteration
          return null;
        }
        return currentList;
      } catch (Exception ex) {
        if (FireStoreUtils.shouldRetry(ex, true)) {
          logger.warn("Retry-able error in firestore future get - message: " + ex.getMessage());
        } else {
          throw new FileSystemExecutionException("get batch execution exception", ex);
        }
      }

      TimeUnit.SECONDS.sleep(SLEEP_SECONDS);
    }
    throw new FileSystemExecutionException("get batch failed - no more retries");
  }
}

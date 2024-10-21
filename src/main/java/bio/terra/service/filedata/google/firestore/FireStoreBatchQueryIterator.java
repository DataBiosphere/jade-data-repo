package bio.terra.service.filedata.google.firestore;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FireStoreBatchQueryIterator {
  private static final Logger logger = LoggerFactory.getLogger(FireStoreBatchQueryIterator.class);

  private final Query baseQuery;
  private final int batchSize;
  private List<QueryDocumentSnapshot> currentList;
  private int count;
  private int totalSize;
  private final int offset;
  private final int limit;
  private final FireStoreUtils fireStoreUtils;
  private final Firestore firestore;

  /**
   * Construct and iterator over a query with a specific batch size.
   *
   * @param baseQuery the base query
   * @param batchSize the size of batches to request
   */
  public FireStoreBatchQueryIterator(
      Query baseQuery, int batchSize, FireStoreUtils fireStoreUtils) {
    this(baseQuery, batchSize, fireStoreUtils, 0, Integer.MAX_VALUE);
  }

  public FireStoreBatchQueryIterator(
      Query baseQuery, int batchSize, FireStoreUtils fireStoreUtils, int offset, int limit) {
    this.baseQuery = baseQuery;
    this.firestore = baseQuery.getFirestore();
    this.batchSize = batchSize;
    this.currentList = null;
    this.offset = offset;
    this.limit = limit;
    this.fireStoreUtils = fireStoreUtils;
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
      int queryLimit = Math.min(limit, batchSize);
      query = baseQuery.offset(offset).limit(queryLimit);
    } else {
      int listSize = currentList.size();
      if (totalSize == limit || listSize < batchSize) {
        // We delivered our last list on the previous call
        return null;
      }
      int currentBatchSize = Math.min(limit - totalSize, batchSize);
      QueryDocumentSnapshot lastDoc = currentList.get(listSize - 1);
      query = baseQuery.startAfter(lastDoc).limit(currentBatchSize);
    }

    // Get the next batch
    logger.info("Retrieving batch {} with batch size of {}", count, batchSize);
    count++;

    currentList =
        fireStoreUtils.runTransactionWithRetry(
            firestore,
            xn -> xn.get(query).get().getDocuments(),
            "getBatch",
            "Retrieving batch " + count + " with batch size of " + batchSize);
    totalSize += currentList.size();

    return currentList;
  }
}

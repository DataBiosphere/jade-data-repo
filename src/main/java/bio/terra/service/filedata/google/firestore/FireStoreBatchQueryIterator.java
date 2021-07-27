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
  private FireStoreUtils fireStoreUtils;
  private Firestore firestore;

  /**
   * Construct and iterator over a query with a specific batch size.
   *
   * @param baseQuery the base query
   * @param batchSize the size of batches to request
   */
  public FireStoreBatchQueryIterator(
      Query baseQuery, int batchSize, FireStoreUtils fireStoreUtils) {
    this.baseQuery = baseQuery;
    this.firestore = baseQuery.getFirestore();
    this.batchSize = batchSize;
    this.currentList = null;
    this.count = 0;
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

    currentList =
        fireStoreUtils.runTransactionWithRetry(
            firestore,
            xn -> xn.get(query).get().getDocuments(),
            "getBatch",
            "Retrieving batch " + count + " with batch size of " + batchSize);

    return currentList;
  }
}

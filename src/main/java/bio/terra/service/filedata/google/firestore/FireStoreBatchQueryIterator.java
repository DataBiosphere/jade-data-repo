package bio.terra.service.filedata.google.firestore;


import bio.terra.service.filedata.exception.FileSystemExecutionException;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class FireStoreBatchQueryIterator {
    private static final Logger logger = LoggerFactory.getLogger(FireStoreBatchQueryIterator.class);

    private final Query baseQuery;
    private final int batchSize;
    private final Transaction transaction;
    private List<QueryDocumentSnapshot> currentList;
    private int count;

    /**
     * Construct and iterator over a query with a specific batch size.
     * @param baseQuery the base query
     * @param batchSize the size of batches to request
     */
    public FireStoreBatchQueryIterator(Query baseQuery, int batchSize, Transaction transaction) {
        this.baseQuery = baseQuery;
        this.batchSize = batchSize;
        this.transaction = transaction;
        this.currentList = null;
        this.count = 0;
    }

    public FireStoreBatchQueryIterator(Query baseQuery, int batchSize) {
        this(baseQuery, batchSize, null);
    }
    /**
     * Get the next batch of documents
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

        // Get the next batch; use the transaction if we have it
        logger.info("Retrieving batch {} with batch size of {}", count, batchSize);
        count++;
        ApiFuture<QuerySnapshot> querySnapshot;
        if (transaction == null) {
            querySnapshot = query.get();
        } else {
            querySnapshot = transaction.get(query);
        }

        try {
            currentList = querySnapshot.get().getDocuments();
        } catch (ExecutionException ex) {
            throw new FileSystemExecutionException("has reference - execution exception", ex);
        }
        if (currentList.size() == 0) {
            // Nothing to return so we at the end of the iteration
            return null;
        }
        return currentList;
    }
}

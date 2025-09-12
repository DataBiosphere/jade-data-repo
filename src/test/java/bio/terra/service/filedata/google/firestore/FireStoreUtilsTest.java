package bio.terra.service.filedata.google.firestore;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import org.junit.jupiter.api.Test;
import java.util.List;

public class FireStoreUtilsTest {

    @Test
    void testCollectionHasDocuments_returnsTrueWhenDocumentsExist() throws Exception {
        Firestore firestore = mock(Firestore.class);
        Query query = mock(Query.class);
        FireStoreUtils fireStoreUtils = mock(FireStoreUtils.class);

        // Mock FireStoreUtils to call real method
        doCallRealMethod().when(fireStoreUtils).collectionHasDocuments(any(), any());

        // Mock runTransactionWithRetry to return 1 (documents exist)
        when(fireStoreUtils.runTransactionWithRetry(any(), any(), anyString(), anyString()))
            .thenReturn(1);

        boolean result = fireStoreUtils.collectionHasDocuments(firestore, query);
        assertTrue(result);
    }

    @Test
    void testCollectionHasDocuments_returnsFalseWhenNoDocumentsExist() throws Exception {
        Firestore firestore = mock(Firestore.class);
        Query query = mock(Query.class);
        FireStoreUtils fireStoreUtils = mock(FireStoreUtils.class);

        doCallRealMethod().when(fireStoreUtils).collectionHasDocuments(any(), any());
        when(fireStoreUtils.runTransactionWithRetry(any(), any(), anyString(), anyString()))
            .thenReturn(0);

        boolean result = fireStoreUtils.collectionHasDocuments(firestore, query);
        assertFalse(result);
    }


    @Test
    void testGetCollectionDocuments_DocumentsExist() throws Exception {
        Firestore firestore = mock(Firestore.class);
        Query query = mock(Query.class);
        QueryDocumentSnapshot doc1 = mock(QueryDocumentSnapshot.class);
        FireStoreUtils fireStoreUtils = mock(FireStoreUtils.class);

        // Mock FireStoreUtils to call real method
        doCallRealMethod().when(fireStoreUtils).getCollectionDocuments(firestore, query);

        // Mock runTransactionWithRetry to return documents
        when(fireStoreUtils.runTransactionWithRetry(eq(firestore), any(), eq("getCollectionDocuments"),
         eq("Querying firestore and retrieving documents from collection")))
            .thenReturn(List.of(doc1));

        List<QueryDocumentSnapshot> result = fireStoreUtils.getCollectionDocuments(firestore, query);
        assertEquals(List.of(doc1), result);
    }

    @Test
    void testGetCollectionDocuments_NoDocumentsExist() throws Exception {
        Firestore firestore = mock(Firestore.class);
        Query query = mock(Query.class);
        FireStoreUtils fireStoreUtils = mock(FireStoreUtils.class);

        doCallRealMethod().when(fireStoreUtils).collectionHasDocuments(firestore, query);
        when(fireStoreUtils.runTransactionWithRetry(eq(firestore), any(), eq("collectionHasDocuments"),
        eq("Querying firestore and checking if collection is empty")))
            .thenReturn(0);

        boolean result = fireStoreUtils.collectionHasDocuments(firestore, query);
        assertFalse(result);
    }
}

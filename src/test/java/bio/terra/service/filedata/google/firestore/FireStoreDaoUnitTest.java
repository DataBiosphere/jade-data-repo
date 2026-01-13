package bio.terra.service.filedata.google.firestore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import bio.terra.common.category.Unit;
import bio.terra.service.configuration.ConfigurationService;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class FireStoreDaoUnitTest {

  @Mock private FireStoreDirectoryDao directoryDao;
  @Mock private FireStoreFileDao fileDao;
  @Mock private FireStoreUtils fireStoreUtils;
  @Mock private ConfigurationService configurationService;
  @Mock private FireStoreProject fireStoreProject;
  @Mock private Firestore firestore;
  @Mock private CollectionReference collectionReference;
  @Mock private Query query;
  @Mock private QuerySnapshot querySnapshot;
  @Mock private QueryDocumentSnapshot document1;
  @Mock private QueryDocumentSnapshot document2;
  @Mock private QueryDocumentSnapshot document3;

  private FireStoreDao fireStoreDao;
  private static final String PROJECT_ID = "test-project";
  private static final String COLLECTION_NAME = "test-collection";
  private static final int BATCH_SIZE = 2;

  @BeforeEach
  void setUp() {
    fireStoreDao =
        new FireStoreDao(
            directoryDao, fileDao, fireStoreUtils, configurationService, null); // performanceLogger

    when(configurationService.getParameterValue(any())).thenReturn(BATCH_SIZE);
    when(firestore.collection(COLLECTION_NAME)).thenReturn(collectionReference);
    when(collectionReference.limit(anyInt())).thenReturn(query);
    when(fireStoreProject.getFirestore()).thenReturn(firestore);
  }

  @Test
  void testProcessCollectionWithPagination_EmptyCollection() throws Exception {
    // Setup: Empty collection
    List<QueryDocumentSnapshot> emptyBatch = List.of();
    when(fireStoreUtils.runTransactionWithRetry(
            eq(firestore), any(), eq("processCollectionWithPagination"), anyString()))
        .thenReturn(emptyBatch);

    // Mock FireStoreProject static method
    try (MockedStatic<FireStoreProject> mockedStatic = mockStatic(FireStoreProject.class)) {
      mockedStatic.when(() -> FireStoreProject.get(PROJECT_ID)).thenReturn(fireStoreProject);

      // Execute
      AtomicInteger processedCount = new AtomicInteger(0);
      fireStoreDao.processCollectionWithPagination(
          PROJECT_ID,
          COLLECTION_NAME,
          doc -> {
            processedCount.incrementAndGet();
          });

      // Verify
      assertEquals(0, processedCount.get());
      verify(fireStoreUtils, times(1))
          .runTransactionWithRetry(
              eq(firestore), any(), eq("processCollectionWithPagination"), anyString());
    }
  }

  @Test
  void testProcessCollectionWithPagination_LargeCollection() throws Exception {
    // Setup: 5 documents across 3 batches (2, 2, 1)
    QueryDocumentSnapshot doc1 = mock(QueryDocumentSnapshot.class);
    QueryDocumentSnapshot doc2 = mock(QueryDocumentSnapshot.class);
    QueryDocumentSnapshot doc3 = mock(QueryDocumentSnapshot.class);
    QueryDocumentSnapshot doc4 = mock(QueryDocumentSnapshot.class);
    QueryDocumentSnapshot doc5 = mock(QueryDocumentSnapshot.class);

    List<QueryDocumentSnapshot> batch1 = List.of(doc1, doc2);
    List<QueryDocumentSnapshot> batch2 = List.of(doc3, doc4);
    List<QueryDocumentSnapshot> batch3 = List.of(doc5);
    List<QueryDocumentSnapshot> emptyBatch = List.of();

    when(fireStoreUtils.runTransactionWithRetry(
            eq(firestore), any(), eq("processCollectionWithPagination"), anyString()))
        .thenReturn(batch1, batch2, batch3, emptyBatch);

    // Mock startAfter for subsequent batches
    Query query2 = mock(Query.class);
    Query query3 = mock(Query.class);
    when(query.startAfter(doc2)).thenReturn(query2);
    when(query2.limit(BATCH_SIZE)).thenReturn(query2);
    when(query2.startAfter(doc4)).thenReturn(query3);
    when(query3.limit(BATCH_SIZE)).thenReturn(query3);

    // Mock FireStoreProject static method
    try (MockedStatic<FireStoreProject> mockedStatic = mockStatic(FireStoreProject.class)) {
      mockedStatic.when(() -> FireStoreProject.get(PROJECT_ID)).thenReturn(fireStoreProject);

      // Execute
      List<QueryDocumentSnapshot> processedDocuments = new ArrayList<>();
      fireStoreDao.processCollectionWithPagination(
          PROJECT_ID,
          COLLECTION_NAME,
          doc -> {
            processedDocuments.add(doc);
          });

      // Verify
      assertEquals(5, processedDocuments.size());
      verify(fireStoreUtils, times(4))
          .runTransactionWithRetry(
              eq(firestore), any(), eq("processCollectionWithPagination"), anyString());
    }
  }

  @Test
  void testProcessCollectionWithPagination_InterruptedException() throws Exception {
    // Setup: Throw InterruptedException from runTransactionWithRetry
    when(fireStoreUtils.runTransactionWithRetry(
            eq(firestore), any(), eq("processCollectionWithPagination"), anyString()))
        .thenThrow(new InterruptedException("Test interruption"));

    // Mock FireStoreProject static method
    try (MockedStatic<FireStoreProject> mockedStatic = mockStatic(FireStoreProject.class)) {
      mockedStatic.when(() -> FireStoreProject.get(PROJECT_ID)).thenReturn(fireStoreProject);

      // Execute & Verify
      org.junit.jupiter.api.Assertions.assertThrows(
          InterruptedException.class,
          () ->
              fireStoreDao.processCollectionWithPagination(PROJECT_ID, COLLECTION_NAME, doc -> {}));
    }
  }

  @Test
  void testProcessCollectionWithPagination_DocumentProcessorThrowsInterruptedException()
      throws Exception {
    // Setup: First batch with documents, processor throws InterruptedException
    List<QueryDocumentSnapshot> firstBatch = List.of(document1, document2);
    when(fireStoreUtils.runTransactionWithRetry(
            eq(firestore), any(), eq("processCollectionWithPagination"), anyString()))
        .thenReturn(firstBatch);

    // Mock FireStoreProject static method
    try (MockedStatic<FireStoreProject> mockedStatic = mockStatic(FireStoreProject.class)) {
      mockedStatic.when(() -> FireStoreProject.get(PROJECT_ID)).thenReturn(fireStoreProject);

      // Execute & Verify: DocumentProcessor throws InterruptedException
      org.junit.jupiter.api.Assertions.assertThrows(
          InterruptedException.class,
          () ->
              fireStoreDao.processCollectionWithPagination(
                  PROJECT_ID,
                  COLLECTION_NAME,
                  doc -> {
                    throw new InterruptedException("Processor interrupted");
                  }));
    }
  }

  @Test
  void testProcessCollectionWithPagination_BatchSizeEqualsDocumentCount() throws Exception {
    // Setup: Batch size equals document count (2 docs, batch size 2)
    // First batch has exactly batchSize documents, second batch is empty
    List<QueryDocumentSnapshot> fullBatch = List.of(document1, document2);
    List<QueryDocumentSnapshot> emptyBatch = List.of();

    when(fireStoreUtils.runTransactionWithRetry(
            eq(firestore), any(), eq("processCollectionWithPagination"), anyString()))
        .thenReturn(fullBatch, emptyBatch);

    // Mock startAfter for second batch (empty)
    Query queryWithStartAfter = mock(Query.class);
    when(query.startAfter(document2)).thenReturn(queryWithStartAfter);
    when(queryWithStartAfter.limit(BATCH_SIZE)).thenReturn(queryWithStartAfter);

    // Mock FireStoreProject static method
    try (MockedStatic<FireStoreProject> mockedStatic = mockStatic(FireStoreProject.class)) {
      mockedStatic.when(() -> FireStoreProject.get(PROJECT_ID)).thenReturn(fireStoreProject);

      // Execute
      List<QueryDocumentSnapshot> processedDocuments = new ArrayList<>();
      fireStoreDao.processCollectionWithPagination(
          PROJECT_ID,
          COLLECTION_NAME,
          doc -> {
            processedDocuments.add(doc);
          });

      // Verify: All documents processed, empty batch detected correctly
      assertEquals(2, processedDocuments.size());
      assertThat(processedDocuments.get(0), equalTo(document1));
      assertThat(processedDocuments.get(1), equalTo(document2));

      // Verify: Two calls - first batch and empty batch check
      verify(fireStoreUtils, times(2))
          .runTransactionWithRetry(
              eq(firestore), any(), eq("processCollectionWithPagination"), anyString());
      verify(query, times(1)).startAfter(document2); // Called for second batch (empty)
    }
  }
}

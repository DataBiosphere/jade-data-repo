package bio.terra.service.filedata.flight.delete;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.exception.FileDependencyException;
import bio.terra.service.filedata.exception.FileSystemAbortTransactionException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;



@ExtendWith(MockitoExtension.class)
public class DeleteFileLookupStepTest {

    @Mock private FireStoreDao fileDao;
    @Mock private FireStoreDependencyDao dependencyDao;
    @Mock private Dataset dataset;
    @Mock private FlightContext context;
    @Mock private FlightMap workingMap;
    @Mock private FireStoreFile fireStoreFile;

    private static final String FILE_ID = "test-file-id";
    private DeleteFileLookupStep step;

    @BeforeEach
    void setUp() {
        step = new DeleteFileLookupStep(fileDao, FILE_ID, dataset, dependencyDao);
        when(context.getWorkingMap()).thenReturn(workingMap);
    }

    @Test
    void testDoStep_FileAlreadyInWorkingMap_NoDependencies_Success() throws InterruptedException {
        when(workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class)).thenReturn(fireStoreFile);
        when(fireStoreFile.getFileId()).thenReturn(FILE_ID);
        when(dependencyDao.getFileSnapshotReferences(dataset, FILE_ID)).thenReturn(Collections.emptyList());

        StepResult result = step.doStep(context);

        assertEquals(StepStatus.STEP_RESULT_SUCCESS, result.getStepStatus());
        verify(fileDao, never()).lookupFile(dataset, FILE_ID);
        verify(workingMap, never()).put(eq(FileMapKeys.FIRESTORE_FILE), any());
    }

    @Test
    void testDoStep_FileNotInWorkingMap_LookupSuccessful_NoDependencies_Success() throws InterruptedException {
        when(workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class)).thenReturn(null);
        when(fileDao.lookupFile(dataset, FILE_ID)).thenReturn(fireStoreFile);
        when(fireStoreFile.getFileId()).thenReturn(FILE_ID);
        when(dependencyDao.getFileSnapshotReferences(dataset, FILE_ID)).thenReturn(Collections.emptyList());

        StepResult result = step.doStep(context);

        assertEquals(StepStatus.STEP_RESULT_SUCCESS, result.getStepStatus());
        verify(fileDao).lookupFile(dataset, FILE_ID);
        verify(workingMap).put(FileMapKeys.FIRESTORE_FILE, fireStoreFile);
    }

    @Test
    void testDoStep_FileNotFound_Success() throws InterruptedException {
        when(workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class)).thenReturn(null);
        when(fileDao.lookupFile(dataset, FILE_ID)).thenReturn(null);

        StepResult result = step.doStep(context);

        assertEquals(StepStatus.STEP_RESULT_SUCCESS, result.getStepStatus());
        verify(fileDao).lookupFile(dataset, FILE_ID);
        verify(workingMap, never()).put(eq(FileMapKeys.FIRESTORE_FILE), any());
        verify(dependencyDao, never()).getFileSnapshotReferences(any(), any());
    }

    @Test
    void testDoStep_FileHasDependencies_ThrowsException() throws InterruptedException {
        List<String> snapshotIds = Arrays.asList("snapshot1", "snapshot2");
        when(workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class)).thenReturn(fireStoreFile);
        when(fireStoreFile.getFileId()).thenReturn(FILE_ID);
        when(dependencyDao.getFileSnapshotReferences(dataset, FILE_ID)).thenReturn(snapshotIds);

        FileDependencyException exception = assertThrows(FileDependencyException.class, 
            () -> step.doStep(context));

        assertTrue(exception.getMessage().contains("snapshot1, snapshot2"));
        assertTrue(exception.getMessage().contains("File is used by at least one snapshot"));
    }

    @Test
    void testDoStep_FileSystemAbortTransactionException_ReturnsRetry() throws InterruptedException {
        FileSystemAbortTransactionException abortException = new FileSystemAbortTransactionException("Abort");
        when(workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class)).thenReturn(null);
        when(fileDao.lookupFile(dataset, FILE_ID)).thenThrow(abortException);

        StepResult result = step.doStep(context);

        assertEquals(StepStatus.STEP_RESULT_FAILURE_RETRY, result.getStepStatus());
        assertEquals(abortException, result.getException().orElse(null));
    }
}


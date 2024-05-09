package bio.terra.service.filedata.flight.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestFileDirectoryStepTest {

  @Mock private FireStoreDao fireStoreDaoService;

  private static final Dataset DATASET = new Dataset();

  private static final UUID FILE_UUID = UUID.randomUUID();

  private void runTest(String ingestFileAction) throws Exception {
    IngestFileDirectoryStep step = new IngestFileDirectoryStep(fireStoreDaoService, DATASET);
    FlightContext flightContext = mock(FlightContext.class);
    FlightMap workingMap = new FlightMap();
    workingMap.put(FileMapKeys.FILE_ID, FILE_UUID.toString());
    workingMap.put(FileMapKeys.INGEST_FILE_ACTION, ingestFileAction);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    assertEquals(StepResult.getStepResultSuccess(), step.undoStep(flightContext));
  }

  @Test
  void testCreateEntryUndoStep() throws Exception {
    when(fireStoreDaoService.deleteDirectoryEntry(DATASET, FILE_UUID.toString())).thenReturn(true);
    runTest("createEntry");
  }

  @Test
  void testCheckEntryUndoStep() throws Exception {
    runTest("checkEntry");

    // Verify that delete was not called on checkEntry undo.
    verifyNoInteractions(fireStoreDaoService);
  }
}

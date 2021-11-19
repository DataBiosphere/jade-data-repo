package bio.terra.service.filedata.flight.ingest;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class IngestFileDirectoryStepTest extends TestCase {

  @MockBean private FireStoreDao fireStoreDaoService;

  @MockBean private Dataset dataset;

  private final UUID fileUuid = UUID.randomUUID();

  private void runTest(String ingestFileAction) throws Exception {
    given(fireStoreDaoService.deleteDirectoryEntry(dataset, fileUuid.toString())).willReturn(true);

    IngestFileDirectoryStep step = new IngestFileDirectoryStep(fireStoreDaoService, dataset);
    FlightContext flightContext = mock(FlightContext.class);
    FlightMap workingMap = new FlightMap();
    workingMap.put(FileMapKeys.FILE_ID, fileUuid.toString());
    workingMap.put(FileMapKeys.INGEST_FILE_ACTION, ingestFileAction);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    assertEquals(StepResult.getStepResultSuccess(), step.undoStep(flightContext));
  }

  @Test
  public void testCreateEntryUndoStep() throws Exception {
    runTest("createEntry");

    // Verify that the delete was called on createEntry undo.
    verify(fireStoreDaoService).deleteDirectoryEntry(dataset, fileUuid.toString());
  }

  @Test
  public void testCheckEntryUndoStep() throws Exception {
    runTest("checkEntry");

    // Verify that the delete was not called on checkEntry undo.
    verify(fireStoreDaoService, never()).deleteDirectoryEntry(dataset, fileUuid.toString());
  }
}

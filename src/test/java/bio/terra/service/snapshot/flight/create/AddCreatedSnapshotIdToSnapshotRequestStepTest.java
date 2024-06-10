package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AddCreatedSnapshotIdToSnapshotRequestStepTest {
  @Mock private SnapshotRequestDao snapshotRequestDao;
  private static final UUID SNAPSHOT_REQUEST_ID = UUID.randomUUID();
  private static final UUID CREATED_SNAPSHOT_ID = UUID.randomUUID();
  private AddCreatedSnapshotIdToSnapshotRequestStep step;

  @BeforeEach
  void beforeEach() {
    step =
        new AddCreatedSnapshotIdToSnapshotRequestStep(
            snapshotRequestDao, SNAPSHOT_REQUEST_ID, CREATED_SNAPSHOT_ID);
  }

  @Test
  void doStep() throws InterruptedException {
    StepResult result = step.doStep(null);
    verify(snapshotRequestDao).updateCreatedSnapshotId(SNAPSHOT_REQUEST_ID, CREATED_SNAPSHOT_ID);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }

  @Test
  void undoStep() throws InterruptedException {
    StepResult result = step.undoStep(null);
    verify(snapshotRequestDao).updateCreatedSnapshotId(SNAPSHOT_REQUEST_ID, null);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }
}

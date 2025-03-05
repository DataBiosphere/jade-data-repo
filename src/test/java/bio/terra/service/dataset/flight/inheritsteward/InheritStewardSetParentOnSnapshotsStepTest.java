package bio.terra.service.dataset.flight.inheritsteward;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class InheritStewardSetParentOnSnapshotsStepTest {
  @Mock private SnapshotService snapshotService;
  @Mock private IamService iamService;
  @Mock private FlightContext context;
  private InheritStewardSetParentOnSnapshotsStep step;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_1 = UUID.randomUUID();
  private static final UUID SNAPSHOT_2 = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    step =
        new InheritStewardSetParentOnSnapshotsStep(
            snapshotService, iamService, DATASET_ID, TEST_USER);
  }

  @Test
  void doStep() throws InterruptedException {
    when(snapshotService.enumerateSnapshotIdsForDataset(DATASET_ID, TEST_USER))
        .thenReturn(List.of(SNAPSHOT_1, SNAPSHOT_2));
    assertEquals(step.doStep(context), StepResult.getStepResultSuccess());
    verify(iamService)
        .setResourceParent(
            TEST_USER.getToken(),
            IamResourceType.DATASNAPSHOT,
            SNAPSHOT_1,
            IamResourceType.DATASET,
            DATASET_ID);
    verify(iamService)
        .setResourceParent(
            TEST_USER.getToken(),
            IamResourceType.DATASNAPSHOT,
            SNAPSHOT_2,
            IamResourceType.DATASET,
            DATASET_ID);
  }

  @Test
  void undoStep() throws InterruptedException {
    when(snapshotService.enumerateSnapshotIdsForDataset(DATASET_ID, TEST_USER))
        .thenReturn(List.of(SNAPSHOT_1, SNAPSHOT_2));
    doThrow(new NotFoundException("no parent found"))
        .when(iamService)
        .deleteResourceParent(TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, SNAPSHOT_1);
    assertEquals(step.undoStep(context), StepResult.getStepResultSuccess());
    verify(iamService)
        .deleteResourceParent(TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, SNAPSHOT_2);
  }
}

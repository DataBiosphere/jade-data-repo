package bio.terra.service.dataset.flight.inheritsteward;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SetParentOnSnapshotsStepTest {
  @Mock private SnapshotService snapshotService;
  @Mock private IamService iamService;
  @Mock private FlightContext context;
  private SetParentOnSnapshotsStep step;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_1 = UUID.randomUUID();
  private static final UUID SNAPSHOT_2 = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    step = new SetParentOnSnapshotsStep(snapshotService, iamService, DATASET_ID, TEST_USER);
  }

  @Test
  void doStep() throws InterruptedException {
    List<UUID> snapshotIds = Arrays.asList(SNAPSHOT_1, SNAPSHOT_2);
    FlightMap workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.SNAPSHOT_IDS, snapshotIds);
    when(context.getWorkingMap()).thenReturn(workingMap);
    assertEquals(step.doStep(context), StepResult.getStepResultSuccess());
    for (UUID snapshotId : snapshotIds) {
      verify(iamService)
          .setResourceParent(
              TEST_USER.getToken(),
              IamResourceType.DATASNAPSHOT,
              snapshotId,
              IamResourceType.DATASET,
              DATASET_ID);
    }
  }

  @Test
  void undoStep() throws InterruptedException {
    FullyQualifiedResourceId childSnapshot =
        new FullyQualifiedResourceId()
            .resourceId(SNAPSHOT_1.toString())
            .resourceTypeName(IamResourceType.DATASNAPSHOT.name());
    FullyQualifiedResourceId childDataset =
        new FullyQualifiedResourceId()
            .resourceId(UUID.randomUUID().toString())
            .resourceTypeName(IamResourceType.DATASET.name());
    when(iamService.listResourceChildren(TEST_USER.getToken(), IamResourceType.DATASET, DATASET_ID))
        .thenReturn(List.of(childSnapshot, childDataset));
    assertEquals(step.undoStep(context), StepResult.getStepResultSuccess());
    verify(iamService)
        .deleteResourceParent(TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, SNAPSHOT_1);
    verifyNoMoreInteractions(iamService);
  }
}

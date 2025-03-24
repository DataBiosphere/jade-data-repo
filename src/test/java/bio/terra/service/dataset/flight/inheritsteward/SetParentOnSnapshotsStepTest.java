package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SetParentOnSnapshotsStepTest {
  @Mock private IamService iamService;
  @Mock private FlightContext context;
  private SetParentOnSnapshotsStep step;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_1 = UUID.randomUUID();
  private static final UUID SNAPSHOT_2 = UUID.randomUUID();

  interface DoOrUndo {
    StepResult apply(FlightContext t) throws Exception;
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStep(boolean inheritSteward) throws Exception {
    step = new SetParentOnSnapshotsStep(iamService, DATASET_ID, TEST_USER, inheritSteward);
    verifySetParent(step::doStep, inheritSteward);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void undoStep(boolean inheritSteward) throws Exception {
    step = new SetParentOnSnapshotsStep(iamService, DATASET_ID, TEST_USER, inheritSteward);
    verifySetParent(step::undoStep, !inheritSteward);
  }

  private void verifySetParent(DoOrUndo doOrUndo, boolean inheritSteward) throws Exception {
    List<UUID> snapshotIds = Arrays.asList(SNAPSHOT_1, SNAPSHOT_2);
    FlightMap workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.SNAPSHOT_IDS, snapshotIds);
    when(context.getWorkingMap()).thenReturn(workingMap);
    assertThat(doOrUndo.apply(context), is(StepResult.getStepResultSuccess()));
    for (UUID snapshotId : snapshotIds) {
      if (inheritSteward) {
        verify(iamService)
            .setResourceParent(
                TEST_USER.getToken(),
                IamResourceType.DATASNAPSHOT,
                snapshotId,
                IamResourceType.DATASET,
                DATASET_ID);
      } else {
        verify(iamService)
            .deleteResourceParent(TEST_USER.getToken(), IamResourceType.DATASNAPSHOT, snapshotId);
      }
    }
  }
}

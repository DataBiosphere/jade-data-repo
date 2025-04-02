package bio.terra.service.dataset.flight.inheritsteward;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.model.FullyQualifiedResourceId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class GetSnapshotIdsStepTest {

  @Mock private SnapshotDao snapshotDao;
  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_1 = UUID.randomUUID();
  private static final UUID SNAPSHOT_2 = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStep(boolean inheritSteward) throws Exception {
    GetSnapshotIdsStep step =
        new GetSnapshotIdsStep(snapshotDao, iamService, TEST_USER, DATASET_ID, inheritSteward);
    FlightMap workingMap = new FlightMap();
    List<UUID> snapshotIds = Arrays.asList(SNAPSHOT_1, SNAPSHOT_2);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    if (inheritSteward) {
      when(snapshotDao.getSnapshotIds(DATASET_ID)).thenReturn(snapshotIds);
    } else {
      List<FullyQualifiedResourceId> children =
          List.of(
              new FullyQualifiedResourceId()
                  .resourceTypeName(IamResourceType.DATASNAPSHOT.getSamResourceName())
                  .resourceId(SNAPSHOT_1.toString()),
              new FullyQualifiedResourceId()
                  .resourceTypeName(IamResourceType.DATASNAPSHOT.getSamResourceName())
                  .resourceId(SNAPSHOT_2.toString()),
              new FullyQualifiedResourceId()
                  .resourceTypeName(IamResourceType.DATASET.getSamResourceName())
                  .resourceId(UUID.randomUUID().toString()));
      when(iamService.listResourceChildren(
              TEST_USER.getToken(), IamResourceType.DATASET, DATASET_ID))
          .thenReturn(children);
    }

    assertEquals(step.doStep(flightContext), StepResult.getStepResultSuccess());
    assertEquals(workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_IDS, List.class), snapshotIds);
  }
}

package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class EnableInheritStewardFlightTest {

  @Mock private ApplicationContext context;
  @Mock private DatasetDao datasetDao;
  @Mock private DatasetService datasetService;
  @Mock private SnapshotService snapshotService;
  @Mock private IamService iamService;
  private FlightMap inputParameters;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void setUp() {
    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET);
    inputParameters.put(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), DATASET_ID);
    inputParameters.put(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.SET_INHERIT_STEWARD);
    inputParameters.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), TEST_USER);
    when(context.getBean(DatasetDao.class)).thenReturn(datasetDao);
    when(context.getBean(DatasetService.class)).thenReturn(datasetService);
    when(context.getBean(SnapshotService.class)).thenReturn(snapshotService);
    when(context.getBean(IamService.class)).thenReturn(iamService);
  }

  @Test
  void testStepsIncluded() {
    var flight = new EnableInheritStewardFlight(inputParameters, context);
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(
        steps,
        contains(
            "InheritStewardSetFlagStep",
            "LockDatasetStep",
            "InheritStewardSetParentOnSnapshotsStep"));
  }

  @Test
  void testParametersForSetFlagStep() {
    try (var mockedStep =
        mockConstruction(
            InheritStewardSetFlagStep.class,
            (mock, context) -> {
              assertThat((DatasetDao) context.arguments().get(0), equalTo(datasetDao));
              assertThat(
                  "The correct datasetId is passed to the step",
                  (UUID) context.arguments().get(1),
                  equalTo(DATASET_ID));
              assertThat(
                  "The correct boolean flag is passed to the step",
                  (boolean) context.arguments().get(2),
                  equalTo(true));
            })) {
      new EnableInheritStewardFlight(inputParameters, context);
    }
  }

  @Test
  void testParametersForLockDatasetStep() {
    try (var mockedStep =
        mockConstruction(
            LockDatasetStep.class,
            (mock, context) -> {
              assertThat((DatasetService) context.arguments().get(0), equalTo(datasetService));
              assertThat(
                  "The correct datasetId is passed to the step",
                  (UUID) context.arguments().get(1),
                  equalTo(DATASET_ID));
              assertThat(
                  "The correct shared lock boolean flag is passed to the step",
                  (boolean) context.arguments().get(2),
                  equalTo(false));
            })) {
      new EnableInheritStewardFlight(inputParameters, context);
    }
  }

  @Test
  void testParametersForSetParentsStep() {
    try (var mockedStep =
        mockConstruction(
            InheritStewardSetParentOnSnapshotsStep.class,
            (mock, context) -> {
              assertThat((SnapshotService) context.arguments().get(0), equalTo(snapshotService));
              assertThat((IamService) context.arguments().get(1), equalTo(iamService));
              assertThat(
                  "The correct datasetId is passed to the step",
                  (UUID) context.arguments().get(2),
                  equalTo(DATASET_ID));
              assertThat((AuthenticatedUserRequest) context.arguments().get(3), equalTo(TEST_USER));
            })) {
      new EnableInheritStewardFlight(inputParameters, context);
    }
  }
}

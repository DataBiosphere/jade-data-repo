package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
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
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
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
  @Mock private SnapshotDao snapshotDao;
  @Mock private ResourceService resourceService;
  @Mock private SnapshotService snapshotService;
  @Mock private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  @Mock private DatasetService datasetService;
  @Mock private IamService iamService;
  private final FlightMap inputParameters = new FlightMap();

  private static final String CUSTODIAN_EMAIL = "custodian email";
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void setUp() {
    inputParameters.put(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET);
    inputParameters.put(DatasetWorkingMapKeys.DATASET_ID, DATASET_ID);
    inputParameters.put(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.SET_INHERIT_STEWARD);
    inputParameters.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), TEST_USER);
    inputParameters.put(JobMapKeys.CUSTODIAN_EMAIL.getKeyName(), CUSTODIAN_EMAIL);
    when(context.getBean(DatasetDao.class)).thenReturn(datasetDao);
    when(context.getBean(SnapshotDao.class)).thenReturn(snapshotDao);
    when(context.getBean(DatasetService.class)).thenReturn(datasetService);
    when(context.getBean(ResourceService.class)).thenReturn(resourceService);
    when(context.getBean(SnapshotService.class)).thenReturn(snapshotService);
    when(context.getBean(BigQuerySnapshotPdao.class)).thenReturn(bigQuerySnapshotPdao);
    when(context.getBean(IamService.class)).thenReturn(iamService);
  }

  @Test
  void allSteps() {
    var flight = new EnableInheritStewardFlight(inputParameters, context);
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(
        steps,
        contains(
            "LockDatasetStep",
            "SetInheritStewardFlagStep",
            "GetSnapshotIdsStep",
            "SetParentOnSnapshotsStep",
            "GetSnapshotGoogleProjectIdsStep",
            "SetAuthBqJobUserStep",
            "SetAuthTabularAclStep",
            "UnlockDatasetStep"));
  }

  @Test
  void lockDatasetStep() {
    try (var mockStep =
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
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void setFlagStep() {
    try (var mockStep =
        mockConstruction(
            SetInheritStewardFlagStep.class,
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
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void getSnapshotsStep() {
    try (var mockStep =
        mockConstruction(
            GetSnapshotIdsStep.class,
            (mock, context) -> {
              assertThat((SnapshotDao) context.arguments().get(0), equalTo(snapshotDao));
              assertThat(
                  "The correct datasetId is passed to the step",
                  (UUID) context.arguments().get(1),
                  equalTo(DATASET_ID));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void setParentsStep() {
    try (var mockStep =
        mockConstruction(
            SetParentOnSnapshotsStep.class,
            (mock, context) -> {
              assertThat((IamService) context.arguments().get(0), equalTo(iamService));
              assertThat(
                  "The correct datasetId is passed to the step",
                  (UUID) context.arguments().get(1),
                  equalTo(DATASET_ID));
              assertThat((AuthenticatedUserRequest) context.arguments().get(2), equalTo(TEST_USER));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void getSnapshotGoogleProjectIdsStep() {
    try (var mockStep =
        mockConstruction(
            GetSnapshotGoogleProjectIdsStep.class,
            (mock, context) -> {
              assertThat((SnapshotService) context.arguments().get(0), equalTo(snapshotService));
              assertThat(
                  "The correct datasetId is passed to the step",
                  (UUID) context.arguments().get(1),
                  equalTo(DATASET_ID));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void setAuthBqJobUserStep() {
    try (var mockStep =
        mockConstruction(
            SetAuthGcpUserRolesStep.class,
            (mock, context) -> {
              assertThat((ResourceService) context.arguments().get(0), equalTo(resourceService));
              assertThat(
                  "The correct custodian email is passed to the step",
                  (String) context.arguments().get(1),
                  equalTo(CUSTODIAN_EMAIL));
              assertThat(
                  "The correct boolean flag is passed to the step",
                  (boolean) context.arguments().get(2),
                  equalTo(true));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void unlockDatasetStep() {
    try (var mockStep =
        mockConstruction(
            UnlockDatasetStep.class,
            (mock, context) -> {
              assertThat((DatasetService) context.arguments().get(0), equalTo(datasetService));
              assertThat(
                  "The correct shared lock boolean flag is passed to the step",
                  (boolean) context.arguments().get(1),
                  equalTo(false));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @Test
  void setAuthTabluarAclStep() {
    try (var mockStep =
        mockConstruction(
            SetAuthTabularAclStep.class,
            (mock, context) -> {
              assertThat(
                  (BigQuerySnapshotPdao) context.arguments().get(0), equalTo(bigQuerySnapshotPdao));
              assertThat((SnapshotService) context.arguments().get(1), equalTo(snapshotService));
              assertThat(
                  "The correct custodian email is passed to the step",
                  (String) context.arguments().get(2),
                  equalTo(CUSTODIAN_EMAIL));
              assertThat(
                  "The correct boolean flag is passed to the step",
                  (boolean) context.arguments().get(3),
                  equalTo(true));
            })) {
      //noinspection ResultOfObjectAllocationIgnored
      new EnableInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }
}

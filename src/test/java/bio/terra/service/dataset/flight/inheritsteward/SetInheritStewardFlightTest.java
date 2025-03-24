package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
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
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.LockDatasetStep;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SetInheritStewardFlightTest {

  @Mock private ApplicationContext context;
  @Mock private DatasetDao datasetDao;
  @Mock private SnapshotDao snapshotDao;
  @Mock private ResourceService resourceService;
  @Mock private SnapshotService snapshotService;
  @Mock private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  @Mock private DatasetService datasetService;
  @Mock private IamService iamService;
  @Mock private JournalService journalService;
  private final FlightMap inputParameters = new FlightMap();

  private static final String CUSTODIAN_EMAIL = "custodian email";
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void setUp() {
    inputParameters.put(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET);
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
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
    when(context.getBean(JournalService.class)).thenReturn(journalService);
  }

  @Test
  void allStepsTrue() {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), true);
    var flight = new SetInheritStewardFlight(inputParameters, context);
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(
        steps,
        contains(
            "LockDatasetStep",
            "SetInheritStewardFlagStep",
            "GetSnapshotIdsStep",
            "SetParentOnSnapshotsStep",
            "SetAuthGcpUserRolesStep",
            "SetAuthTabularAclStep",
            "AdjustStewardMembersStep",
            "UnlockDatasetStep",
            "JournalRecordUpdateEntryStep"));
  }

  @Test
  void allStepsFalse() {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), false);
    var flight = new SetInheritStewardFlight(inputParameters, context);
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(
        steps,
        contains(
            "LockDatasetStep",
            "GetSnapshotIdsStep",
            "SetParentOnSnapshotsStep",
            "SetAuthGcpUserRolesStep",
            "SetAuthTabularAclStep",
            "AdjustStewardMembersStep",
            "SetInheritStewardFlagStep",
            "UnlockDatasetStep",
            "JournalRecordUpdateEntryStep"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void lockDatasetStep(boolean inheritSteward) {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), inheritSteward);
    try (var mockStep =
        mockConstruction(
            LockDatasetStep.class,
            (mock, context) ->
                assertThat(context.arguments(), contains(datasetService, DATASET_ID, false)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SetInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setFlagStep(boolean inheritSteward) {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), inheritSteward);
    try (var mockStep =
        mockConstruction(
            SetInheritStewardFlagStep.class,
            (mock, context) ->
                assertThat(
                    context.arguments(), contains(datasetDao, DATASET_ID, inheritSteward)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SetInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void getSnapshotIdsStep(boolean inheritSteward) {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), inheritSteward);
    try (var mockStep =
        mockConstruction(
            GetSnapshotIdsStep.class,
            (mock, context) ->
                assertThat(
                    context.arguments(),
                    contains(snapshotDao, iamService, TEST_USER, DATASET_ID, inheritSteward)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SetInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setParentsStep(boolean inheritSteward) {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), inheritSteward);
    try (var mockStep =
        mockConstruction(
            SetParentOnSnapshotsStep.class,
            (mock, context) ->
                assertThat(
                    context.arguments(),
                    contains(iamService, DATASET_ID, TEST_USER, inheritSteward)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SetInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setAuthGcpUserRolesStep(boolean inheritSteward) {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), inheritSteward);
    try (var mockStep =
        mockConstruction(
            SetAuthGcpUserRolesStep.class,
            (mock, context) ->
                assertThat(
                    context.arguments(),
                    contains(resourceService, snapshotService, CUSTODIAN_EMAIL, inheritSteward)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SetInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void adjustStewardMembersStep(boolean inheritSteward) {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), inheritSteward);
    try (var mockStep =
        mockConstruction(
            AdjustStewardMembersStep.class,
            (mock, context) ->
                assertThat(context.arguments(), contains(TEST_USER, iamService, inheritSteward)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SetInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void unlockDatasetStep(boolean inheritSteward) {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), inheritSteward);
    try (var mockStep =
        mockConstruction(
            UnlockDatasetStep.class,
            (mock, context) -> assertThat(context.arguments(), contains(datasetService, false)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SetInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setAuthTabluarAclStep(boolean inheritSteward) {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), inheritSteward);
    try (var mockStep =
        mockConstruction(
            SetAuthTabularAclStep.class,
            (mock, context) ->
                assertThat(
                    context.arguments(),
                    contains(
                        bigQuerySnapshotPdao, snapshotService, CUSTODIAN_EMAIL, inheritSteward)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SetInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void journalRecordUpdateEntryStep(boolean inheritSteward) {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), inheritSteward);
    try (var mockStep =
        mockConstruction(
            JournalRecordUpdateEntryStep.class,
            (mock, context) ->
                assertThat(
                    context.arguments(),
                    contains(
                        journalService,
                        TEST_USER,
                        DATASET_ID,
                        IamResourceType.DATASET,
                        "Set inherit steward flag to " + inheritSteward)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SetInheritStewardFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }
}

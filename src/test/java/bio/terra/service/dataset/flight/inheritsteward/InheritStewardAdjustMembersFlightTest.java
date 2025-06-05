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
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightMap;
import java.util.Arrays;
import java.util.List;
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
class InheritStewardAdjustMembersFlightTest {
  @Mock private ApplicationContext context;
  @Mock private SnapshotService snapshotService;
  @Mock private IamService iamService;
  private final FlightMap inputParameters = new FlightMap();

  private static final String CUSTODIAN_EMAIL = "custodian email";
  private static final String STEWARD_EMAIL = "steward email";
  private static final List<String> DATASET_POLICY_EMAILS =
      Arrays.asList(CUSTODIAN_EMAIL, STEWARD_EMAIL);
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void setUp() {
    inputParameters.put(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.DATASET);
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
    inputParameters.put(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.SET_INHERIT_STEWARD);
    inputParameters.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), TEST_USER);
    inputParameters.put(JobMapKeys.DATASET_POLICY_EMAILS.getKeyName(), DATASET_POLICY_EMAILS);
    when(context.getBean(SnapshotService.class)).thenReturn(snapshotService);
    when(context.getBean(IamService.class)).thenReturn(iamService);
  }

  @Test
  void allSteps() {
    inputParameters.put(JobMapKeys.INHERIT_STEWARD.getKeyName(), true);
    var flight = new InheritStewardAdjustMembersFlight(inputParameters, context);
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(steps, contains("GetSnapshotIdsStep", "AdjustStewardMembersStep"));
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
                    contains(
                        snapshotService, iamService, TEST_USER, DATASET_ID, inheritSteward)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new InheritStewardAdjustMembersFlight(inputParameters, context);
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
      new InheritStewardAdjustMembersFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }
}

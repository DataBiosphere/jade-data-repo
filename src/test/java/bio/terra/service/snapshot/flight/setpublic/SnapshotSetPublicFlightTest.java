package bio.terra.service.snapshot.flight.setpublic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotSetPublicFlightTest {
  @Mock private IamService iamService;
  @Mock private ApplicationContext context;
  private final FlightMap inputParameters = new FlightMap();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void setUp() {
    inputParameters.put(JobMapKeys.SNAPSHOT_ID.getKeyName(), SNAPSHOT_ID);
    inputParameters.put(JobMapKeys.AUTH_USER_INFO.getKeyName(), TEST_USER);
    when(context.getBean(IamService.class)).thenReturn(iamService);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void allSteps(boolean setPublic) {
    inputParameters.put(JobMapKeys.SET_PUBLIC.getKeyName(), setPublic);
    var flight = new SnapshotSetPublicFlight(inputParameters, context);
    var steps = FlightTestUtils.getStepNames(flight);
    assertThat(steps, contains("SetSnapshotPublicStep"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setSnapshotPublicStep(boolean setPublic) {
    inputParameters.put(JobMapKeys.SET_PUBLIC.getKeyName(), setPublic);
    try (var mockStep =
        mockConstruction(
            SetSnapshotPublicStep.class,
            (mock, context) ->
                assertThat(
                    context.arguments(),
                    contains(SNAPSHOT_ID, setPublic, TEST_USER, iamService)))) {
      //noinspection ResultOfObjectAllocationIgnored
      new SnapshotSetPublicFlight(inputParameters, context);
      assertThat(mockStep.constructed(), hasSize(1));
    }
  }
}

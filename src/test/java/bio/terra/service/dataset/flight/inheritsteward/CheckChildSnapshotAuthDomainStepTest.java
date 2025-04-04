package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Arrays;
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
class CheckChildSnapshotAuthDomainStepTest {
  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext context;
  private CheckChildSnapshotAuthDomainStep step;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_1 = UUID.randomUUID();
  private static final UUID SNAPSHOT_2 = UUID.randomUUID();
  private List<UUID> snapshotIds;

  @BeforeEach
  void setup() {
    step = new CheckChildSnapshotAuthDomainStep(snapshotService, TEST_USER);
    snapshotIds = Arrays.asList(SNAPSHOT_1, SNAPSHOT_2);
    FlightMap workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.SNAPSHOT_IDS, snapshotIds);
    when(context.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void doStep_NoAuthDomains() {
    when(snapshotService.retrieveAuthDomains(SNAPSHOT_1, TEST_USER)).thenReturn(List.of());
    when(snapshotService.retrieveAuthDomains(SNAPSHOT_2, TEST_USER)).thenReturn(List.of());
    assertThat(step.doStep(context), is(StepResult.getStepResultSuccess()));
    for (UUID snapshotId : snapshotIds) {
      verify(snapshotService).retrieveAuthDomains(snapshotId, TEST_USER);
    }
  }

  @Test
  void doStep_WithAuthDomains() {
    when(snapshotService.retrieveAuthDomains(SNAPSHOT_1, TEST_USER))
        .thenReturn(List.of("authDomain1"));
    when(snapshotService.retrieveAuthDomains(SNAPSHOT_2, TEST_USER)).thenReturn(List.of());
    var result = step.doStep(context);
    assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_FAILURE_FATAL));
    assertThat(
        "Correct exception is returned",
        result.getException().get().getMessage(),
        is(
            "The following snapshots have auth domains: ["
                + SNAPSHOT_1
                + "]. Please delete any snapshots with auth domains before setting inherit steward."));
  }
}

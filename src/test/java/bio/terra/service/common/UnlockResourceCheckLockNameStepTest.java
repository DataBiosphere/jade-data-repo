package bio.terra.service.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class UnlockResourceCheckLockNameStepTest {
  private static final IamResourceType IAM_RESOURCE_TYPE = IamResourceType.DATASET;
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private static final String LOCK_NAME = "lock-name";
  private static final String OTHER_LOCK_NAME = "other-lock-name";
  private static final Boolean IS_SHARED_LOCK = false;
  @Mock private FlightContext flightContext;

  @Test
  void doStep_success() throws InterruptedException {
    var workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    UnlockResourceCheckLockNameStep step = new TestStep(List.of(LOCK_NAME, OTHER_LOCK_NAME));

    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));
    assertThat(
        workingMap.get(DatasetWorkingMapKeys.IS_SHARED_LOCK, Boolean.class),
        equalTo(IS_SHARED_LOCK));
  }

  private static Stream<Arguments> doStep_failure() {
    String noLocksMessage = "Dataset %s is not locked.".formatted(RESOURCE_ID);

    List<String> existingLocksOnResource = List.of(OTHER_LOCK_NAME);
    String noMatchingLockMessage =
        """
            Dataset %s has no lock named '%s'.
            Do you mean to remove one of these existing locks instead?
            """
            .formatted(RESOURCE_ID, LOCK_NAME);

    return Stream.of(
        arguments(List.of(), new ResourceLockConflict(noLocksMessage)),
        arguments(
            existingLocksOnResource,
            new ResourceLockConflict(noMatchingLockMessage, existingLocksOnResource)));
  }

  @ParameterizedTest
  @MethodSource
  void doStep_failure(List<String> existingResourceLocks, ResourceLockConflict expectedException)
      throws InterruptedException {
    UnlockResourceCheckLockNameStep step = new TestStep(existingResourceLocks);

    StepResult stepResult = step.doStep(flightContext);
    assertThat(stepResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));

    Optional<Exception> maybeException = stepResult.getException();
    assertTrue(maybeException.isPresent());
    Exception exception = maybeException.get();
    assertThat(exception, instanceOf(ResourceLockConflict.class));
    ResourceLockConflict resourceLockConflict = (ResourceLockConflict) exception;
    assertThat(resourceLockConflict.getMessage(), equalTo(expectedException.getMessage()));
    assertThat(resourceLockConflict.getCauses(), equalTo(expectedException.getCauses()));
  }

  private static class TestStep extends UnlockResourceCheckLockNameStep {
    private final List<String> actualLocks;

    public TestStep(List<String> actualLockNames) {
      super(IAM_RESOURCE_TYPE, RESOURCE_ID, LOCK_NAME);
      this.actualLocks = actualLockNames;
    }

    @Override
    protected List<String> getLocks() {
      return actualLocks;
    }

    @Override
    protected boolean isSharedLock(String lockName) {
      return IS_SHARED_LOCK;
    }
  }
}

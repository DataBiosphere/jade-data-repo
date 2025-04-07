package bio.terra.service.snapshot.flight.setpublic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.ForbiddenException;
import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SetSnapshotPublicStepTest {

  @Mock IamService iamService;
  @Mock private FlightContext context;
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private SetSnapshotPublicStep step;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStep(boolean setPublic) throws InterruptedException {
    step = new SetSnapshotPublicStep(SNAPSHOT_ID, setPublic, TEST_USER, iamService);
    assertEquals(StepResult.getStepResultSuccess(), step.doStep(context));
    verifySuccess(setPublic);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void undoStep(boolean setPublic) throws InterruptedException {
    step = new SetSnapshotPublicStep(SNAPSHOT_ID, setPublic, TEST_USER, iamService);
    assertEquals(StepResult.getStepResultSuccess(), step.undoStep(context));
    verifySuccess(!setPublic);
  }

  private void verifySuccess(boolean setPublic) {
    verify(iamService)
        .setPolicyPublicV2(
            TEST_USER.getToken(),
            IamResourceType.DATASNAPSHOT,
            SNAPSHOT_ID,
            IamRole.READER.name(),
            setPublic);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStepForbidden(boolean setPublic) throws InterruptedException {
    step = new SetSnapshotPublicStep(SNAPSHOT_ID, setPublic, TEST_USER, iamService);
    var expectedException = throwForbiddenException(setPublic);
    var result = step.doStep(context);
    assertEquals(StepStatus.STEP_RESULT_FAILURE_FATAL, result.getStepStatus());
    assertEquals(expectedException.getMessage(), result.getException().get().getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void undoStepForbidden(boolean setPublic) throws InterruptedException {
    step = new SetSnapshotPublicStep(SNAPSHOT_ID, setPublic, TEST_USER, iamService);
    var expectedException = throwForbiddenException(!setPublic);
    var result = step.undoStep(context);
    assertEquals(StepStatus.STEP_RESULT_FAILURE_FATAL, result.getStepStatus());
    assertEquals(expectedException.getMessage(), result.getException().get().getMessage());
  }

  private ForbiddenException expectedForbiddenException(boolean setPublic, Exception e) {
    throwException(setPublic, e);
    return new ForbiddenException(
        "User is not authorized to set this resource as public, contact Terra support for assistance.",
        e);
  }

  private void throwException(boolean setPublic, Exception e) {
    doThrow(e)
        .when(iamService)
        .setPolicyPublicV2(
            TEST_USER.getToken(),
            IamResourceType.DATASNAPSHOT,
            SNAPSHOT_ID,
            IamRole.READER.name(),
            setPublic);
  }

  @NotNull
  private ForbiddenException throwForbiddenException(boolean setPublic) {
    var exception = new ForbiddenException("Forbidden");
    return expectedForbiddenException(setPublic, exception);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStepNotFound(boolean setPublic) throws InterruptedException {
    step = new SetSnapshotPublicStep(SNAPSHOT_ID, setPublic, TEST_USER, iamService);
    var expectedException = throwNotFoundException(setPublic);
    var result = step.doStep(context);
    assertEquals(StepStatus.STEP_RESULT_FAILURE_FATAL, result.getStepStatus());
    assertEquals(expectedException.getMessage(), result.getException().get().getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void undoStepNotFound(boolean setPublic) throws InterruptedException {
    step = new SetSnapshotPublicStep(SNAPSHOT_ID, setPublic, TEST_USER, iamService);
    var expectedException = throwNotFoundException(!setPublic);
    var result = step.undoStep(context);
    assertEquals(StepStatus.STEP_RESULT_FAILURE_FATAL, result.getStepStatus());
    assertEquals(expectedException.getMessage(), result.getException().get().getMessage());
  }

  @NotNull
  private ForbiddenException throwNotFoundException(boolean setPublic) {
    var exception = new NotFoundException("NotFound");
    return expectedForbiddenException(setPublic, exception);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStepOtherException(boolean setPublic) throws InterruptedException {
    step = new SetSnapshotPublicStep(SNAPSHOT_ID, setPublic, TEST_USER, iamService);
    var exception = throwException(setPublic);
    var result = step.doStep(context);
    assertEquals(StepStatus.STEP_RESULT_FAILURE_FATAL, result.getStepStatus());
    assertEquals(exception.getMessage(), result.getException().get().getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void undoStepOtherException(boolean setPublic) throws InterruptedException {
    step = new SetSnapshotPublicStep(SNAPSHOT_ID, setPublic, TEST_USER, iamService);
    var exception = throwException(!setPublic);
    var result = step.undoStep(context);
    assertEquals(StepStatus.STEP_RESULT_FAILURE_FATAL, result.getStepStatus());
    assertEquals(exception.getMessage(), result.getException().get().getMessage());
  }

  private Exception throwException(boolean setPublic) {
    var exception = new InternalServerErrorException("Other error");
    throwException(setPublic, exception);
    return exception;
  }
}

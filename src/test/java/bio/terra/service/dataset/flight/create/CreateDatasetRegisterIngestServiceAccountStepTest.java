package bio.terra.service.dataset.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamInternalServerErrorException;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.auth.iam.exception.IamUnauthorizedException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class CreateDatasetRegisterIngestServiceAccountStepTest {
  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;
  private CreateDatasetRegisterIngestServiceAccountStep step;

  @BeforeEach
  void setup() {
    step = new CreateDatasetRegisterIngestServiceAccountStep(iamService);
    FlightMap workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.SERVICE_ACCOUNT_EMAIL, "email");
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void doStep() throws InterruptedException {
    assertThat(step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(iamService).registerUser("email");
  }

  @Test
  void doStep_Retry401() throws InterruptedException {
    doThrow(new IamUnauthorizedException("Unauthorized")).when(iamService).registerUser("email");
    assertThat(
        step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
  }

  @Test
  void doStep_Retry404() throws InterruptedException {
    doThrow(new IamNotFoundException(new Throwable("Not Found")))
        .when(iamService)
        .registerUser("email");
    assertThat(
        step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
  }

  @Test
  void doStep_500() {
    doThrow(new IamInternalServerErrorException("Internal Server Error"))
        .when(iamService)
        .registerUser("email");
    assertThrows(IamInternalServerErrorException.class, () -> step.doStep(flightContext));
  }
}

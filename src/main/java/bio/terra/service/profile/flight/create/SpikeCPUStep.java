package bio.terra.service.profile.flight.create;

import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.math.BigInteger;

public class SpikeCPUStep implements Step {

  public SpikeCPUStep() {}

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      factorialHavingLargeResult(1000000);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }

  public void factorialHavingLargeResult(int n) {
    BigInteger result = BigInteger.ONE;
    for (int i = 2; i <= n; i++) result = result.multiply(BigInteger.valueOf(i));
  }
}

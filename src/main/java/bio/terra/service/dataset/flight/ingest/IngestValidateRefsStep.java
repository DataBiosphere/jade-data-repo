package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.dataset.exception.InvalidFileRefException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class IngestValidateRefsStep implements Step {
  private static final int MAX_ERROR_REF_IDS = 20;

  public StepResult handleInvalidRefs(Set<String> invalidRefIds) {
    int invalidIdCount = invalidRefIds.size();
    if (invalidIdCount != 0) {
      // Made a string buffer to appease findbugs; it saw + in the loop and said "bad!"
      StringBuffer errorMessage = new StringBuffer("Invalid file ids found during ingest (");

      List<String> errorDetails = new ArrayList<>();
      int count = 0;
      for (String badId : invalidRefIds) {
        errorDetails.add(badId);
        count++;
        if (count > MAX_ERROR_REF_IDS) {
          errorMessage.append(MAX_ERROR_REF_IDS + "out of ");
          break;
        }
      }
      errorMessage.append(invalidIdCount + " returned in details)");
      throw new InvalidFileRefException(errorMessage.toString(), errorDetails);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // The update will update row ids that are null, so it can be restarted on failure.
    return StepResult.getStepResultSuccess();
  }
}

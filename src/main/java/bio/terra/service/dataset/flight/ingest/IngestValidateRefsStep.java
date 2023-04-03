package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.dataset.exception.InvalidFileRefException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public interface IngestValidateRefsStep extends Step {
  int MAX_ERROR_REF_IDS = 20;

  default StepResult handleInvalidRefs(Set<InvalidRefId> invalidRefIds) {
    int invalidIdCount = invalidRefIds.size();
    if (invalidIdCount != 0) {
      // Made a string buffer to appease findbugs; it saw + in the loop and said "bad!"
      StringBuilder errorMessage = new StringBuilder("Invalid file ids found during ingest (");

      List<String> errorDetails = new ArrayList<>();
      int count = 0;
      for (InvalidRefId badId : invalidRefIds) {
        if (count >= MAX_ERROR_REF_IDS) {
          errorMessage.append(MAX_ERROR_REF_IDS + "out of ");
          break;
        }
        errorDetails.add(badId.toString());
        count++;
      }
      errorMessage.append(invalidIdCount).append(" returned in details)");
      throw new InvalidFileRefException(errorMessage.toString(), errorDetails);
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public default StepResult undoStep(FlightContext context) {
    // The update will update row ids that are null, so it can be restarted on failure.
    return StepResult.getStepResultSuccess();
  }

  record InvalidRefId(String refId, String columnName) {}
}

package bio.terra.common;

import bio.terra.model.ErrorModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import com.google.common.annotations.VisibleForTesting;
import org.springframework.http.HttpStatus;

public abstract class BaseStep implements Step {
  @StepOutput protected Object response;
  @StepOutput protected HttpStatus statusCode;

  private FlightContext context;

  @Override
  public final StepResult doStep(FlightContext context)
      throws InterruptedException, RetryException {
    this.context = context;
    StepUtils.readInputs(this, context);
    try {
      return perform();
    } finally {
      StepUtils.writeOutputs(this, context);
    }
  }

  @Override
  public final StepResult undoStep(FlightContext context) throws InterruptedException {
    this.context = context;
    StepUtils.readInputs(this, context);
    return undo();
  }

  public abstract StepResult perform() throws InterruptedException, RetryException;

  public StepResult undo() throws InterruptedException {
    // Many steps aren't undoable.
    return StepResult.getStepResultSuccess();
  }

  protected void setErrorResponse(String message, HttpStatus responseStatus) {
    ErrorModel errorModel = new ErrorModel().message(message);
    setResponse(errorModel, responseStatus);
  }

  protected void setResponse(Object responseObject, HttpStatus responseStatus) {
    response = responseObject;
    statusCode = responseStatus;
  }

  protected void setResponse(Object responseObject) {
    response = responseObject;
    statusCode = HttpStatus.OK;
  }

  protected FlightContext getContext() {
    return context;
  }

  protected String getFlightId() {
    return context.getFlightId();
  }

  @VisibleForTesting
  public Object getResponse() {
    return response;
  }

  @VisibleForTesting
  public HttpStatus getStatusCode() {
    return statusCode;
  }
}

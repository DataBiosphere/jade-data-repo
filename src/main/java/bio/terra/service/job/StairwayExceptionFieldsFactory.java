package bio.terra.service.job;

import bio.terra.common.exception.ErrorReportException;
import bio.terra.service.job.exception.ExceptionSerializerException;
import bio.terra.service.job.exception.JobExecutionException;
import bio.terra.service.job.exception.StepExecutionException;
import bio.terra.stairway.Step;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

public class StairwayExceptionFieldsFactory {
  private static final String CONTACT_TEAM_MESSAGE = "Please contact Terra Support for help.";

  public static StairwayExceptionFields fromException(Exception ex) {
    String stepName = getStepNameFromStairwayException(ex);

    Optional<String> fieldExceptionClassName = Optional.empty();
    var fieldErrorCode = HttpStatus.INTERNAL_SERVER_ERROR.value();
    var isApiErrorReportException = false;

    if (ex instanceof ErrorReportException errorReportException) {
      fieldExceptionClassName = Optional.of(ex.getClass().getName());
      fieldErrorCode = errorReportException.getStatusCode().value();
      isApiErrorReportException = true;
    }

    if (stepName == null) {
      return new StairwayExceptionFields()
          .setClassName(fieldExceptionClassName.orElse(JobExecutionException.class.getName()))
          .setDataRepoException(true)
          .setMessage(getJobExceptionMessage(ex))
          .setErrorDetails(getJobExceptionDetails(ex))
          .setErrorCode(fieldErrorCode)
          .setApiErrorReportException(isApiErrorReportException);
    }

    return new StairwayExceptionFields()
        .setClassName(fieldExceptionClassName.orElse(StepExecutionException.class.getName()))
        .setDataRepoException(true)
        .setMessage(getStepExceptionMessage(stepName, ex))
        .setErrorDetails(getStepExceptionDetails(ex))
        .setErrorCode(fieldErrorCode)
        .setApiErrorReportException(isApiErrorReportException);
  }

  private static String getStepNameFromStairwayException(Exception ex) {
    String failedStepName = null;
    for (StackTraceElement ste : ex.getStackTrace()) {
      try {
        Class<?> cls = Class.forName(ste.getClassName());
        if (Step.class.isAssignableFrom(cls)) {
          failedStepName = cls.getSimpleName();
          break;
        }
      } catch (ClassNotFoundException e) {
        throw new ExceptionSerializerException(
            "All steps in a stacktrace should be able to be found", e);
      }
    }
    return failedStepName;
  }

  private static String getJobExceptionMessage(Exception exception) {
    if (StringUtils.isNotBlank(exception.getMessage())) {
      return exception.getMessage();
    }
    return String.format(
        "Encountered %s while running the job", exception.getClass().getSimpleName());
  }

  private static List<String> getJobExceptionDetails(Exception exception) {
    List<String> errorReportCauses = getErrorReportExceptionCauses(exception);
    if (errorReportCauses != null) {
      return errorReportCauses;
    }
    return List.of("The job failed, but not while running a step", CONTACT_TEAM_MESSAGE);
  }

  private static String getStepExceptionMessage(String stepName, Exception exception) {
    if (StringUtils.isNotBlank(exception.getMessage())) {
      return exception.getMessage();
    }
    return String.format(
        "Encountered %s while running %s.", exception.getClass().getSimpleName(), stepName);
  }

  private static List<String> getStepExceptionDetails(Exception exception) {
    List<String> errorReportCauses = getErrorReportExceptionCauses(exception);
    if (errorReportCauses != null) {
      return errorReportCauses;
    }
    return List.of("The step failed for an unknown reason.", CONTACT_TEAM_MESSAGE);
  }

  private static List<String> getErrorReportExceptionCauses(Exception exception) {
    if (exception instanceof ErrorReportException errorReportException) {
      List<String> causes = new ArrayList<>();
      if (!errorReportException.getCauses().isEmpty()) {
        causes.addAll(errorReportException.getCauses());
      }
      if (errorReportException.getCause() != null) {
        causes.add(errorReportException.getCause().getMessage());
      }
      causes.removeIf(StringUtils::isEmpty);
      return causes;
    } else if (StringUtils.isNotEmpty(exception.getMessage())) {
      return List.of(exception.getMessage());
    }
    return List.of();
  }
}

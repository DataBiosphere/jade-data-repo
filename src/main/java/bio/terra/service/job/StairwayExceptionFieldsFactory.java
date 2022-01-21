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

public class StairwayExceptionFieldsFactory {

  private static String CONTACT_TEAM_MESSAGE = "Please contact the TDR team for help.";

  public static StairwayExceptionFields fieldsFromException(Exception ex) {
    Optional<String> stepName = getStepNameFromStairwayException(ex);

    Optional<String> fieldExceptionClassName = Optional.empty();
    if (ErrorReportException.class.isAssignableFrom(ex.getClass())) {
      fieldExceptionClassName = Optional.of(ex.getClass().getName());
    }

    if (stepName.isEmpty()) {
      return new StairwayExceptionFields()
          .setClassName(fieldExceptionClassName.orElse(JobExecutionException.class.getName()))
          .setDataRepoException(true)
          .setMessage(getJobExceptionMessage(ex))
          .setErrorDetails(getJobExceptionDetails(ex));
    }

    return new StairwayExceptionFields()
        .setClassName(fieldExceptionClassName.orElse(StepExecutionException.class.getName()))
        .setDataRepoException(true)
        .setMessage(getStepExceptionMessage(stepName.get(), ex))
        .setErrorDetails(getStepExceptionDetails(ex));
  }

  private static Optional<String> getStepNameFromStairwayException(Exception ex) {
    String failedStepName = null;
    for (StackTraceElement ste : ex.getStackTrace()) {
      try {
        Class<?> cls = Class.forName(ste.getClassName());
        if (Step.class.isAssignableFrom(cls)) {
          // Java 17 will make this casting unnecessary
          failedStepName = cls.getSimpleName();
          break;
        }
      } catch (ClassNotFoundException e) {
        throw new ExceptionSerializerException(
            "All steps in a stacktrace should be able to be found", e);
      }
    }
    return Optional.ofNullable(failedStepName);
  }

  private static String getJobExceptionMessage(Exception exception) {
    if (StringUtils.isNotBlank(exception.getMessage())) {
      return exception.getMessage();
    }
    return String.format(
        "Encountered a %s while running the job", exception.getClass().getSimpleName());
  }

  private static List<String> getJobExceptionDetails(Exception exception) {
    if (ErrorReportException.class.isAssignableFrom(exception.getClass())) {
      ErrorReportException errorReportException = (ErrorReportException) exception;
      return errorReportException.getCauses();
    } else {
      List<String> details = new ArrayList<>();
      details.add("The job failed, but not while running a step");
      details.add(CONTACT_TEAM_MESSAGE);
      return details;
    }
  }

  private static String getStepExceptionMessage(String stepName, Exception exception) {
    if (StringUtils.isNotBlank(exception.getMessage())) {
      return exception.getMessage();
    }
    return String.format(
        "Encountered %s while running %s.", exception.getClass().getSimpleName(), stepName);
  }

  private static List<String> getStepExceptionDetails(Exception exception) {
    if (ErrorReportException.class.isAssignableFrom(exception.getClass())) {
      ErrorReportException errorReportException = (ErrorReportException) exception;
      return errorReportException.getCauses();
    } else {
      List<String> details = new ArrayList<>();
      if ("IamUnauthorizedException".equals(exception.getClass().getSimpleName())) {
        details.add(
            "If this is a long-running job, the user token may have timed out. "
                + "Consider breaking the job into smaller jobs if possible.");
        details.add(
            "There may be a problem with bucket access. "
                + "Has the user (or PET) account been granted access to all necessary files?");
      }
      return details;
    }
  }
}

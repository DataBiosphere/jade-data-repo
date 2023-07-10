package bio.terra.app.controller;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.ConflictException;
import bio.terra.common.exception.ErrorReportException;
import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.common.exception.ForbiddenException;
import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.ServiceUnavailableException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.model.ErrorModel;
import bio.terra.service.auth.iam.sam.SamIam;
import bio.terra.service.job.exception.JobResponseException;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
class GlobalExceptionHandlerTest {
  private GlobalExceptionHandler globalExceptionHandler;
  private GlobalExceptionHandler mockGlobalExceptionHandler;
  private static final String testExceptionMessage = "test exception";

  @BeforeEach
  void setup() {
    globalExceptionHandler = new GlobalExceptionHandler();
    mockGlobalExceptionHandler = Mockito.spy(globalExceptionHandler);
  }

  // Positive test cases: We expect Sentry to capture these exceptions
  @Test
  void catchallHandlerCapturedInSentry() {
    Exception ex = new Exception(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.catchallHandler(ex);
    assertSentryCaptured(ex, errorModel);
  }

  @Test
  void internalServerErrorHandlerCapturedInSentry() {
    ErrorReportException ex = new InternalServerErrorException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.internalServerErrorHandler(ex);
    assertSentryCaptured(ex, errorModel);
  }

  @Test
  void serviceUnavailableHandlerCapturedInSentry() {
    ErrorReportException ex = new ServiceUnavailableException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.serviceUnavailableHandler(ex);
    assertSentryCaptured(ex, errorModel);
  }

  @Test
  void samApiExceptionHandlerCapturedInSentry() {
    ApiException ex = new ApiException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.samApiExceptionHandler(ex);
    ErrorReportException drex = SamIam.convertSAMExToDataRepoEx(ex);
    assertSentryCaptured(drex, errorModel);
  }

  @Test
  void jobResponseExceptionHandlerCapturedInSentry() {
    Exception cause = new JobResponseException(testExceptionMessage);
    Exception ex = new JobResponseException(testExceptionMessage, cause);
    ErrorModel errorModel = mockGlobalExceptionHandler.jobResponseExceptionHandler(ex);
    assertSentryCaptured(cause, errorModel);
  }

  private void assertSentryCaptured(Exception ex, ErrorModel errorModel) {
    verify(mockGlobalExceptionHandler).captureSentryException(any());
    assertThat(
        "exception message is in error model",
        errorModel.getMessage(),
        equalTo(testExceptionMessage));
  }

  // Negative test cases: We do NOT expect for Sentry to capture these exceptions
  @Test
  void notFoundHandlerNotCapturedInSentry() {
    ErrorReportException ex = new NotFoundException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.notFoundHandler(ex);
    assertSentryNotCaptured(ex, errorModel);
  }

  @Test
  void badRequestNotCapturedInSentry() {
    ErrorReportException ex = new BadRequestException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.badRequestHandler(ex);
    assertSentryNotCaptured(ex, errorModel);
  }

  @Test
  void notImplementedHandlerNotCapturedInSentry() {
    ErrorReportException ex = new FeatureNotImplementedException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.notImplementedHandler(ex);
    assertSentryNotCaptured(ex, errorModel);
  }

  @Test
  void conflictHandlerNotCapturedInSentry() {
    ErrorReportException ex = new ConflictException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.conflictHandler(ex);
    assertSentryNotCaptured(ex, errorModel);
  }

  @Test
  void samAuthorizationExceptionNotCapturedInSentry() {
    UnauthorizedException ex = new UnauthorizedException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.samAuthorizationException(ex);
    assertSentryNotCaptured(ex, errorModel);
  }

  @Test
  void forbiddenHandlerNotCapturedInSentry() {
    ErrorReportException ex = new ForbiddenException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.forbiddenHandler(ex);
    assertSentryNotCaptured(ex, errorModel);
  }

  @Test
  void validationExceptionHandlerNotCapturedInSentry() {
    Exception ex = new IllegalArgumentException(testExceptionMessage);
    ErrorModel errorModel = mockGlobalExceptionHandler.validationExceptionHandler(ex);
    assertSentryNotCaptured(ex, errorModel);
  }

  private void assertSentryNotCaptured(Exception ex, ErrorModel errorModel) {
    verify(mockGlobalExceptionHandler, never()).captureSentryException(ex);
    assertThat(
        "exception message is in error model",
        errorModel.getMessage(),
        equalTo(testExceptionMessage));
  }
}

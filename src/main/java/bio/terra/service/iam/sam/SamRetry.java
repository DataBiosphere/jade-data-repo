package bio.terra.service.iam.sam;

import static java.time.Instant.now;

import bio.terra.common.exception.ErrorReportException;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.exception.IamInternalServerErrorException;
import com.google.api.client.http.HttpStatusCodes;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SamRetry {
  private static Logger logger = LoggerFactory.getLogger(SamRetry.class);
  private final Instant operationTimeout;
  private final int retryMaxWait;
  private final ConfigurationService configService;
  private int retrySeconds;

  SamRetry(ConfigurationService configService) {
    this.configService = configService;
    this.retryMaxWait = configService.getParameterValue(ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS);
    this.retrySeconds = configService.getParameterValue(ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS);
    int operationTimeoutSeconds =
        configService.getParameterValue(ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS);
    this.operationTimeout = now().plusSeconds(operationTimeoutSeconds);
  }

  @FunctionalInterface
  public interface SamVoidFunction {
    void apply() throws ApiException, InterruptedException;
  }

  @FunctionalInterface
  public interface SamFunction<R> {
    R apply() throws ApiException, InterruptedException;
  }

  static <T> T retry(ConfigurationService configService, SamFunction<T> function)
      throws InterruptedException {
    SamRetry samRetry = new SamRetry(configService);
    return samRetry.perform(function);
  }

  static void retry(ConfigurationService configService, SamVoidFunction function)
      throws InterruptedException {
    SamRetry samRetry = new SamRetry(configService);
    samRetry.performVoid(function);
  }

  private <T> T perform(SamFunction<T> function) throws InterruptedException {
    while (true) {
      try {
        insertFaultWhenTesting();

        return function.apply();
      } catch (ApiException ex) {
        handleApiException(ex);
      } catch (Exception ex) {
        throw new IamInternalServerErrorException(
            "Unexpected exception type: " + ex.toString(), ex);
      }
      sleepBeforeRetrying();
    }
  }

  private void performVoid(SamVoidFunction function) throws InterruptedException {
    while (true) {
      try {
        insertFaultWhenTesting();

        function.apply();
        return;
      } catch (ApiException ex) {
        handleApiException(ex);
      } catch (Exception ex) {
        throw new IamInternalServerErrorException(
            "Unexpected exception type: " + ex.toString(), ex);
      }
      sleepBeforeRetrying();
    }
  }

  private void handleApiException(ApiException ex) {
    ErrorReportException rex = SamIam.convertSAMExToDataRepoEx(ex);
    if (!(rex instanceof IamInternalServerErrorException)) {
      throw rex;
    }
    logger.info("SamRetry: caught retry-able exception: " + ex);
    // if our operation timeout will happen before we wake up from our next retry
    // sleep, then we give up and re-throw.
    if (operationTimeout.minusSeconds(retrySeconds).isBefore(now())) {
      logger.error("SamRetry: operation timed out after " + operationTimeout.toString());
      throw rex;
    }
  }

  private void sleepBeforeRetrying() throws InterruptedException {
    logger.info("SamRetry: sleeping " + retrySeconds + " seconds");
    TimeUnit.SECONDS.sleep(retrySeconds);

    retrySeconds = retrySeconds + retrySeconds;
    if (retrySeconds > retryMaxWait) {
      retrySeconds = retryMaxWait;
    }
  }

  private void insertFaultWhenTesting() throws Exception {
    // Simulate a socket timeout for testing
    configService.fault(
        ConfigEnum.SAM_TIMEOUT_FAULT,
        () -> {
          throw new ApiException(
              "fault insertion", HttpStatusCodes.STATUS_CODE_SERVER_ERROR, null, null);
        });
  }
}

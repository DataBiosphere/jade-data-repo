package bio.terra.service.iam.sam;


import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.exception.IamInternalServerErrorException;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static java.time.Instant.now;

class SamRetry {
    private static Logger logger = LoggerFactory.getLogger(SamRetry.class);
    private final Instant operationTimeout;
    private final int retryMaxWait;
    private int retrySeconds;

    SamRetry(ConfigurationService configService) {
        this.retryMaxWait =
            configService.getParameterValue(ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS);
        this.retrySeconds =
            configService.getParameterValue(ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS);
        int operationTimeoutSeconds =
            configService.getParameterValue(ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS);
        this.operationTimeout = now().plusSeconds(operationTimeoutSeconds);
    }

    <T> T perform(SamFunction<T> function) {
        while (true) {
            try {
                return function.apply();
            } catch (ApiException ex) {
                RuntimeException rex = SamIam.convertSAMExToDataRepoEx(ex);
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

            // Retry
            try {
                logger.info("SamRetry: sleeping " + retrySeconds + " seconds");
                TimeUnit.SECONDS.sleep(retrySeconds);
            } catch (InterruptedException iex) {
                Thread.currentThread().interrupt();
                throw new IamInternalServerErrorException("SamIam operation was interrupted");
            }

            retrySeconds = retrySeconds + retrySeconds;
            if (retrySeconds > retryMaxWait) {
                retrySeconds = retryMaxWait;
            }
        }
    }
}

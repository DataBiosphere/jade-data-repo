package bio.terra.service.iam.sam;


import bio.terra.common.exception.DataRepoException;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.exception.IamInternalServerErrorException;
import com.google.api.client.http.HttpStatusCodes;
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
    private final ConfigurationService configService;
    private int retrySeconds;

    SamRetry(ConfigurationService configService) {
        this.configService = configService;
        this.retryMaxWait =
            configService.getParameterValue(ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS);
        this.retrySeconds =
            configService.getParameterValue(ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS);
        int operationTimeoutSeconds =
            configService.getParameterValue(ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS);
        this.operationTimeout = now().plusSeconds(operationTimeoutSeconds);
    }

    <T> T perform(SamFunction<T> function) throws InterruptedException {
        while (true) {
            try {
                // Simulate a socket timeout for testing
                configService.fault(ConfigEnum.SAM_TIMEOUT_FAULT, () -> {
                    throw new ApiException("fault insertion", HttpStatusCodes.STATUS_CODE_SERVER_ERROR, null, null);
                });

                return function.apply();

            } catch (ApiException ex) {
                DataRepoException rex = SamIam.convertSAMExToDataRepoEx(ex);
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
            } catch (Exception ex) {
                throw new IamInternalServerErrorException("Unexpected exception type: " + ex.toString(), ex);
            }

            // Retry
            logger.info("SamRetry: sleeping " + retrySeconds + " seconds");
            TimeUnit.SECONDS.sleep(retrySeconds);

            retrySeconds = retrySeconds + retrySeconds;
            if (retrySeconds > retryMaxWait) {
                retrySeconds = retryMaxWait;
            }
        }
    }
}

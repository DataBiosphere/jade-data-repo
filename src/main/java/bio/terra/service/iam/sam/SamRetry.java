package bio.terra.service.iam.sam;


import bio.terra.service.iam.exception.IamInternalServerErrorException;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static java.time.Instant.now;

class SamRetry {
    private static Logger logger = LoggerFactory.getLogger(SamRetry.class);
    private final SamConfiguration samConfig;
    private final Instant operationTimeout;
    private int retrySeconds;

    SamRetry(SamConfiguration samConfig) {
        this.samConfig = samConfig;
        this.operationTimeout = now().plusSeconds(samConfig.getOperationTimeoutSeconds());
        this.retrySeconds = samConfig.getRetryInitialWaitSeconds();
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
            if (retrySeconds > samConfig.getRetryMaximumWaitSeconds()) {
                retrySeconds = samConfig.getRetryMaximumWaitSeconds();
            }
        }
    }
}

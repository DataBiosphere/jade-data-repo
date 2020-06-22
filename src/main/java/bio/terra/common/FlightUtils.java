package bio.terra.common;

import bio.terra.model.ErrorModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import com.google.cloud.bigquery.BigQueryException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

/**
 * Common methods for building flights
 */
public final class FlightUtils {
    private static final Logger logger = LoggerFactory.getLogger(FlightUtils.class);

    private FlightUtils() {

    }

    /**
     * Build an error model and set it as the response
     *
     * @param context
     * @param message
     * @param responseStatus
     */
    public static void setErrorResponse(FlightContext context, String message, HttpStatus responseStatus) {
        ErrorModel errorModel = new ErrorModel().message(message);
        setResponse(context, errorModel, responseStatus);
    }

    /**
     * Set the response and status code in the result map.
     *
     * @param context        flight context
     * @param responseObject response object to set
     * @param responseStatus status code to set
     */
    public static void setResponse(FlightContext context, Object responseObject, HttpStatus responseStatus) {
        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), responseObject);
        workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), responseStatus);
    }

    /**
     * Common logic for deciding if a BigQuery exception is a retry-able IAM propagation error.
     * There is not a specific reason code for the IAM setPolicy failed error. This check is a bit fragile.
     *
     * @param ex exception to test
     * @return true if exception is likely to be an IAM Propagation error.
     */
    public static boolean isBigQueryIamPropagationError(BigQueryException ex) {
        if (StringUtils.equals(ex.getReason(), "invalid") &&
            StringUtils.contains(ex.getMessage(), "IAM setPolicy")) {
            logger.info("Caught probable IAM propagation error - retrying", ex);
            return true;
        }
        return false;
    }
}

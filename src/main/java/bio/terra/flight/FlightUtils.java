package bio.terra.flight;

import bio.terra.model.ErrorModel;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;

import java.util.UUID;

/**
 * Common methods for building flights
 */
public final class FlightUtils {
    private FlightUtils() {

    }

    /**
     * Build an error model and set it as the response
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
        workingMap.put(JobMapKeys.RESPONSE, responseObject);
        workingMap.put(JobMapKeys.STATUS_CODE, responseStatus);
    }

    public static String randomizeName(String baseName) {
        String name = baseName + UUID.randomUUID().toString();
        return StringUtils.replaceChars(name, '-', '_');
    }

    public static String randomizeNameInfix(String baseName, String infix) {
        return randomizeName(baseName + infix);
    }
}


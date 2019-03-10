package bio.terra.flight;

import bio.terra.model.ErrorModel;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightState;
import org.springframework.http.HttpStatus;

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
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), responseObject);
        workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), responseStatus);
    }

    /**
     *
     * There are four cases to handle here:
     * <ol>
     *     <li> Flight is still running. We set the flag in the flight respponse and return.</li>
     *     <li> Successful flight: resultMap RESPONSE is the target class</li>
     *     <li> Failed flight: responseMap RESPONSE is an ErrorModel</li>
     *     <li> Failed flight: no RESPONSE filled in (unhandled exception). We generate
     *     an error response.</li>
     * </ol>
     * @param flightState state of a flight returned from Stairway
     * @return FlightResponse object
     */
    public static FlightResponse makeFlightResponse(FlightState flightState) {
        FlightResponse flightResponse = new FlightResponse();

        if (!flightState.getCompleted().isPresent()) {
            return flightResponse;
        }

        FlightMap resultMap = flightState.getResultMap().orElse(null);
        if (resultMap == null) {
            throw new IllegalStateException("No result map returned from flight");
        }
        HttpStatus returnedStatus = resultMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class);
        Object returnedModel = resultMap.get(JobMapKeys.RESPONSE.getKeyName(), Object.class);

        switch (flightState.getFlightStatus()) {
            case FATAL:
            case ERROR:
                // If the flight failed without supplying a status code and response, then we generate one
                // from the flight error. This handles the case of thrown errors that the step code does
                // not handle.
                if (returnedStatus == null) {
                    returnedStatus = HttpStatus.INTERNAL_SERVER_ERROR;
                }
                if (returnedModel == null) {
                    String msg = flightState.getErrorMessage().orElse("Job failed with no error message!");
                    returnedModel = new ErrorModel().message(msg);
                }
                break;

            case RUNNING:
                // This should never happen
                throw new IllegalStateException("Job marked running but has completion time");

            case SUCCESS:
                if (returnedStatus == null) {
                    // Error: this is a flight coding bug where the status code was not filled in properly.
                    throw new IllegalStateException("No status code returned from flight");
                }
                break;

            default:
                throw new IllegalStateException("Switch default should never be taken");
        }

        flightResponse
            .flightComplete(true)
            .responsePresent(returnedModel != null)
            .errorResponse(returnedModel instanceof ErrorModel)
            .response(returnedModel)
            .statusCode(returnedStatus);
        return flightResponse;
    }
}

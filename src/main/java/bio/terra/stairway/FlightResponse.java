package bio.terra.stairway;

import org.springframework.http.HttpStatus;

/**
 * This class holds the response from a flight and a few flags about the state of
 * the flight.
 */
public class FlightResponse {
    private boolean flightComplete;
    private boolean responsePresent;
    private boolean errorResponse;
    private Object response;
    private HttpStatus statusCode;

    public boolean isFlightComplete() {
        return flightComplete;
    }

    public FlightResponse flightComplete(boolean flightComplete) {
        this.flightComplete = flightComplete;
        return this;
    }

    public boolean isResponsePresent() {
        return responsePresent;
    }

    public FlightResponse responsePresent(boolean responsePresent) {
        this.responsePresent = responsePresent;
        return this;
    }

    public boolean isErrorResponse() {
        return errorResponse;
    }

    public FlightResponse errorResponse(boolean errorResponse) {
        this.errorResponse = errorResponse;
        return this;
    }

    public Object getResponse() {
        return response;
    }

    public FlightResponse response(Object response) {
        this.response = response;
        return this;
    }

    public HttpStatus getStatusCode() {
        return statusCode;
    }

    public FlightResponse statusCode(HttpStatus statusCode) {
        this.statusCode = statusCode;
        return this;
    }
}

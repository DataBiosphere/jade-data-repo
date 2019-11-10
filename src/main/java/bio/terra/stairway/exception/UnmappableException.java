package bio.terra.stairway.exception;

/** This exception is used to wrap exceptions that do not meet the requirements for
 * being recreated from their serialized form in FlightDao. See ExceptionFields class.
 */
public class UnmappableException extends StairwayException {
    public UnmappableException(String message) {
        super(message);
    }
}

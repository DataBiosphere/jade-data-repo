package bio.terra.stairway;

/**
 * There are a great many forms of exceptions. Unfortunately, neither Jackson nor GSON are able to
 * properly serialize and deserialize them; typically, because the JSON serializers require the existence
 * of a default (no arguments) constructor.
 *
 * Further, the users of Stairway - like Terra Data Repository for a random example - have specific data in
 * their exceptions that needs to be preserved. Rather than try to make a general purpose solution in Stairway
 * we use this interface.
 *
 * Users of Stairway create a class that implements this serializer and pass it in on Stairway construction.
 * Stairway then uses the methods herein to serialize and deserialize exceptions.
 */
public interface ExceptionSerializer {
    /**
     * Given an exception return a serialized exception in the form of a string.
     *
     * @param exception the exception to serialize; it may be null
     * @return it is permissible to return null for the serialized form
     */
    String serialize(Exception exception);

    /**
     *
     * @param serializedException the string form of the exception; it may be null
     * @return returns null if there is no exception
     */
    Exception deserialize(String serializedException);
}

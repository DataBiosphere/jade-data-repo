package bio.terra.stairway;

/**
 * Implementations of the interface are used to wait between retries and
 * inform the caller when no more retries should be attempted.
 */
public interface RetryRule {
    /**
     * {@link Flight} calls the {@code initialize} method before starting {@code do} or {@code undo} execution
     * for the first time. The retry rule is shared by {@code do} and {@code undo}, so needs to be re-initialized
     * each time. Since flights are executed sequentially, that means you can share one instance of a retry rule
     * for multiple steps in a flight.
     */
    void initialize();

    /**
     * {@link Flight} calls the {@code retrySleep} method after a retry-able error from a step.
     * The {@code retrySleep} may sleep for some amount of time and return true
     *
     * @return true indicates the caller should attempt another retry. False indicates that no more
     * retries should be attempted.
     */
    boolean retrySleep() throws InterruptedException;
}

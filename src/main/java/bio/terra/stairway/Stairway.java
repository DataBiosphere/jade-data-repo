package bio.terra.stairway;

import bio.terra.stairway.exception.FlightException;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.MakeFlightException;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

/**
 * Stairway is the object that drives execution of Flights. The class is constructed
 * with inputs that allow the caller to specify the thread pool, the database source, and the
 * table name stem to use.
 *
 * Each Stairway runs, logs, and recovers independently.
 *
 * There are two techniques you can use to wait for a flight. One is polling by calling getFlightState. That
 * reads the flight state from the stairway database so will report correct state for as long as the flight
 * lives in the database. (Since we haven't implemented pruning, that means forever.) If you poll in this way,
 * then you must explicitly call releaseFlight() so that the in-memory resources for the flight are freed.
 *
 * The other technique is to poll the flight future using the isDone() method or block waiting for the
 * flight to complete using the waitForFlight() method. In this case, the in-memory resources are freed
 * when either method detects that the flight is done.
 */
public class Stairway {
    // For each task we start, we make a task context. It lets us look up the results

    static class TaskContext {
        private FutureTask<FlightState> futureResult;
        private Flight flight;

        TaskContext(FutureTask<FlightState> futureResult, Flight flight) {
            this.futureResult = futureResult;
            this.flight = flight;
        }

        FutureTask<FlightState> getFutureResult() {
            return futureResult;
        }

        Flight getFlight() {
            return flight;
        }
    }

    private ConcurrentHashMap<String, TaskContext> taskContextMap;
    private ExecutorService threadPool;
    private DataSource dataSource;
    private Database database;
    private Object applicationContext;

    public Stairway(ExecutorService threadPool) {
        this(threadPool, null, true, null);
    }

    /**
     *
     * @param threadPool a thread pool must be provided. The caller chooses the type of pool to use.
     * @param dataSource optional - if null, no database operations are performed.
     *                   That is enforced in the {@link Database} class.
     * @param forceCleanStart true will drop any existing stairway database tables and recreate them from scratch.
     *                        false will validate the schema version matches, and recovery any incomplete flights.
     */
    public Stairway(ExecutorService threadPool,
                    DataSource dataSource,
                    boolean forceCleanStart,
                    Object applicationContext) {
        this.threadPool = threadPool;
        this.dataSource = dataSource;
        this.applicationContext = applicationContext;
        this.taskContextMap = new ConcurrentHashMap<>();
        this.database = new Database(dataSource, forceCleanStart);
        if (!forceCleanStart) {
            recoverFlights();
        }
    }

    /**
     * Submit a flight for execution
     *
     * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
     * @param inputParameters key-value map of parameters to the flight
     * @return unique flight id of the submitted flight
     */
    public String submit(Class<? extends Flight> flightClass, FlightMap inputParameters) {
        if (flightClass == null || inputParameters == null) {
            throw new MakeFlightException("Must supply non-null flightClass and inputParameters to submit");
        }
        Flight flight = makeFlight(flightClass, inputParameters);

        // Generate the sequence id as a UUID. We have no dependency on database id generation and no
        // confusion with small integers that might be accidentally valid.
        flight.context().setFlightId(UUID.randomUUID().toString());

        database.submit(flight.context());

        launchFlight(flight);
        return flight.context().getFlightId();
    }

    // Tests if flight is done
    public boolean isDone(String flightId) {
        TaskContext taskContext = lookupFlight(flightId);
        boolean done = taskContext.getFutureResult().isDone();
        if (done) {
            taskContextMap.remove(flightId);
        }

        return done;
    }

    /**
     * Wait for a flight to complete. When it completes, the flight is removed from the taskContextMap.
     *
     * @param flightId
     */
    public void waitForFlight(String flightId) {
        TaskContext taskContext = lookupFlight(flightId);

        try {
            taskContext.getFutureResult().get();
            taskContextMap.remove(flightId);
        } catch (InterruptedException ex) {
            // Someone is shutting down the application
            Thread.currentThread().interrupt();
            throw new FlightException("Stairway was interrupted");
        } catch (ExecutionException ex) {
            throw new FlightException("Unexpected flight exception", ex);
        }
    }

    /**
     * Get the state of a specific flight
     *
     * @param flightId
     * @return FlightState
     */
    public FlightState getFlightState(String flightId) {
        return database.getFlightState(flightId);
    }

    /**
     * Enumerate flights - returns a range of flights ordered by submit time.
     * Note that there can be "jitter" in the paging through flights, if new flights
     * are submitted.
     *
     * @param offset
     * @param limit
     * @return List of FlightState
     */
    public List<FlightState> getFlights(int offset, int limit) {
        return database.getFlights(offset, limit);
    }

    /**
     * Remove the task context for a completed flight. If the flight is not complete or does not
     * exist, the code simply returns.
     *
     * @param flightId
     */
    public void releaseFlight(String flightId) {
        TaskContext taskContext = taskContextMap.get(flightId);
        if (taskContext != null) {
            if (taskContext.getFutureResult().isDone()) {
                taskContextMap.remove(flightId);
            }
        }
    }

    private TaskContext lookupFlight(String flightId) {
        TaskContext taskContext = taskContextMap.get(flightId);
        if (taskContext == null) {
            throw new FlightNotFoundException("Flight '" + flightId + "' not found");
        }
        return taskContext;
    }

    /**
     * Find any incomplete flights and recover them. We overwrite the flight context of this flight
     * with the recovered flight context. The normal constructor path needs to give the input parameters
     * to the flight subclass. This is a case where we don't really want to have the Flight object set up
     * its own context. It is simpler to override it than to make a separate code path for this recovery case.
     *
     * The flightlog records the last operation performed; so we need to set the execution point to the next
     * step index.
     */
    private void recoverFlights() {
        List<FlightContext> flightList = database.recover();
        for (FlightContext flightContext : flightList) {
            Flight flight = makeFlightFromName(flightContext.getFlightClassName(), flightContext.getInputParameters());
            flightContext.nextStepIndex();
            flight.setFlightContext(flightContext);
            launchFlight(flight);
        }
    }

    /**
     * Build the task context to keep track of the running flight.
     * Once it is launched, hook it into the {@link #taskContextMap} so other
     * calls can resolve it.
     *
     * @param flight
     */
    private void launchFlight(Flight flight) {
        // Give the flight the database object so it can properly record its steps
        flight.setDatabase(database);

        // Build the task context to keep track of the running task
        TaskContext taskContext = new TaskContext(new FutureTask<FlightState>(flight), flight);
        threadPool.execute(taskContext.getFutureResult());

        // Now that it is in the pool, hook it into the map so other calls can resolve it.
        taskContextMap.put(flight.context().getFlightId(), taskContext);
    }

    /**
     *  Create a Flight instance given the class name of the derived class of Flight
     * and the input parameters.
     *
     * Note that you can adjust the steps you generate based on the input parameters.
     *
     * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
     * @param inputParameters key-value map of parameters to the flight
     * @return flight object suitable for submitting for execution
     */
    private Flight makeFlight(Class<? extends Flight> flightClass, FlightMap inputParameters) {
        try {
            // Find the flightClass constructor that takes the input parameter map and
            // use it to make the flight.
            Constructor constructor = flightClass.getConstructor(FlightMap.class, Object.class);
            Flight flight = (Flight)constructor.newInstance(inputParameters, applicationContext);
            return flight;
        } catch (InvocationTargetException |
                NoSuchMethodException |
                InstantiationException |
                IllegalAccessException ex) {
            throw new MakeFlightException("Failed to make a flight from class '" + flightClass + "'", ex);
        }
    }

    /**
     * Version of makeFlight that accepts the class name instead of the class
     * object as in {@link #makeFlight}
     *
     * We use the class name to store and retrieve from the database when we recover.
     */
    private Flight makeFlightFromName(String className, FlightMap inputMap) {
        try {
            Class<?> someClass = Class.forName(className);
            if (Flight.class.isAssignableFrom(someClass)) {
                Class<? extends Flight> flightClass = (Class<? extends Flight>) someClass;
                return makeFlight(flightClass, inputMap);
            }
            // Error case
            throw new MakeFlightException("Failed to make a flight from class name '" + className +
                    "' - it is not a subclass of Flight");

        } catch (ClassNotFoundException ex) {
            throw new MakeFlightException("Failed to make a flight from class name '" + className +
                    "'", ex);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("taskContextMap", taskContextMap)
                .append("threadPool", threadPool)
                .append("dataSource", dataSource)
                .append("database", database)
                .append("applicationContext", applicationContext)
                .toString();
    }
}

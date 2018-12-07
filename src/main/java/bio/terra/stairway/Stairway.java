package bio.terra.stairway;

import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.MakeFlightException;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;

/**
 * Stairway is the object that drives execution of Flights. The class is constructed
 * with inputs that allow the caller to specify the thread pool, the database source, and the
 * table name stem to use.
 *
 * Each Stairway runs, logs, and recovers independently. The table name stem allows the
 * Stairway to have its backing store in a database that is used by other things in the application.
 * Also, one database can be used to hold the backing store for more than one Stairway.
 */
public class Stairway {
    // For each task we start, we make a task context. It lets us look up the results

    class TaskContext {
        FutureTask<FlightResult> futureResult;
        Flight flight;
    }
    private Map<String, TaskContext> taskContextMap;

    private ExecutorService threadPool;
    private DataSource dataSource;
    private String nameStem;

    public Stairway(ExecutorService threadPool, DataSource dataSource, String nameStem, boolean forceCleanStart) {
        this.threadPool = threadPool;
        this.dataSource = dataSource;
        this.nameStem = nameStem;
        this.taskContextMap = new HashMap<>();
        startup(forceCleanStart);
    }

    /**
     * Submit a flight for execution
     *
     * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
     * @param inputParameters key-value map of parameters to the flight
     * @return unique flight id of the submitted flight
     */
    public String submit(Class<? extends Flight> flightClass, SafeHashMap inputParameters) {
        if (flightClass == null || inputParameters == null) {
            throw new MakeFlightException("Must supply non-null flightClass and inputParameters to submit");
        }
        Flight flight = makeFlight(flightClass, inputParameters);

        // Generate the sequence id as a UUID. We have no dependency on database id generation and no
        // confusion with small integers that might be accidentally valid.
        flight.context().flightId(UUID.randomUUID().toString());

        // TODO: write flight to database in submitted state

        // Build the task context to keep track of the running task
        TaskContext taskContext = new TaskContext();
        taskContext.flight = flight;
        taskContext.futureResult = new FutureTask<>(flight);

        threadPool.execute(taskContext.futureResult);

        // Now that it is in the pool, hook it into the map so other calls can resolve it.
        taskContextMap.put(flight.context().getFlightId(), taskContext);

        return flight.context().getFlightId();
    }

    // Tests if flight is done
    public boolean isDone(String flightId) {
        TaskContext taskContext = lookupFlight(flightId);
        return taskContext.futureResult.isDone();
    }

    /**
     * Wait for a flight to complete and return the results
     *
     * @param flightId
     * @return FlightResult object with status, possible exception, and result parameters
     */
    public FlightResult getResult(String flightId) {
        TaskContext taskContext = lookupFlight(flightId);

        try {
            FlightResult result = taskContext.futureResult.get();
            return result;

        } catch (InterruptedException ex) {
            // Someone is shutting down the application
            Thread.currentThread().interrupt();
            return FlightResult.flightResultFatal(ex);
        } catch (ExecutionException ex) {
            return FlightResult.flightResultFatal(ex);
        }
    }

    /**
     * Release the results - the caller is done with this flight.
     * Release of a flightId that does not exist is not an error.
     */
    // TODO: REVIEWERS: is separating getResult and release a good idea?
    // The idea of release is to be able to provide the results after a failure/recovery.
    // And explicitly say I am done, allowing cleanup of the database table and the in-memory
    // List of flights. OTOH, I'm not sure we want to clean up the database.
    public void release(String flightId) {
        TaskContext taskContext = taskContextMap.get(flightId);
        if (taskContext != null) {
            taskContextMap.remove(flightId);
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
     * Initialize the sequencer
     *
     * This creates the sequencer tables, if necessary. If forceCleanStart is set, then
     * it ensures that the sequencer tables are emptied. That is useful if you have backed up
     * the database
     *

     *
     */
    private void startup(boolean forceCleanStart) {
        // TODO: implement database clean or recovery
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
    private Flight makeFlight(Class<? extends Flight> flightClass, SafeHashMap inputParameters) {
        try {
            // Find the flightClass constructor that takes the input parameter map and
            // use it to make the flight.
            Constructor constructor = flightClass.getConstructor(SafeHashMap.class);
            Flight flight = (Flight)constructor.newInstance(inputParameters);
            return flight;
        } catch (InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException ex) {
            throw new MakeFlightException("Failed to make a flight from class '" + flightClass + "'", ex);
        }
    }

    /**
     * Version of makeFlight that accepts the class name instead of the class
     * object as in {@link #makeFlight}
     *
     * We use the class name to store and retrieve from the database when we recover.
     */
    private Flight makeFlightFromName(String className, SafeHashMap inputMap) {
        try {
            Class<?> someClass = Class.forName(className);
            if (Flight.class.isAssignableFrom(someClass)) {
                Class<? extends Flight> flightClass = (Class<? extends Flight>) someClass;
                return makeFlight(flightClass, inputMap);
            }
            // Error case
            throw new MakeFlightException("Failed to make a flight from class name '" + className + "' - it is not a subclass of Flight");

        } catch (ClassNotFoundException ex) {
            throw new MakeFlightException("Failed to make a flight from class name '" + className + "'", ex);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("taskContextMap", taskContextMap)
                .append("threadPool", threadPool)
                .append("dataSource", dataSource)
                .append("nameStem", nameStem)
                .toString();
    }
}

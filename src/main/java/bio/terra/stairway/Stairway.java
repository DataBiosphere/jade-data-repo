package bio.terra.stairway;

import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.MakeFlightException;
import org.apache.commons.lang3.builder.ToStringBuilder;

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
 * Each Stairway runs, logs, and recovers independently. The table name stem allows the
 * Stairway to have its backing store in a database that is used by other things in the application.
 * Also, one database can be used to hold the backing store for more than one Stairway.
 */
public class Stairway {
    // For each task we start, we make a task context. It lets us look up the results

    static class TaskContext {
        private FutureTask<FlightResult> futureResult;
        private Flight flight;

        TaskContext(FutureTask<FlightResult> futureResult, Flight flight) {
            this.futureResult = futureResult;
            this.flight = flight;
        }

        FutureTask<FlightResult> getFutureResult() {
            return futureResult;
        }

        Flight getFlight() {
            return flight;
        }

    }

    private ConcurrentHashMap<String, TaskContext> taskContextMap;

    private ExecutorService threadPool;
    private DataSource dataSource;
    private String schemaName;
    private String nameStem;
    private Database database;

    public Stairway(ExecutorService threadPool) {
        this(threadPool, null, null, null, true);
    }

    /**
     *
     * @param threadPool a thread pool must be provided. The caller chooses the type of pool to use.
     * @param dataSource optional - if null, no database operations are performed.
     *                   That is enforced in the {@link Database} class.
     * @param schemaName optional - if null, no schema is created/used.
     * @param nameStem optional - if null, no table prefix is used. Otherwise tables are named like
     *                 nameStem + '_' + tableName
     * @param forceCleanStart true will drop any existing stairway database tables and recreate them from scratch.
     *                        false will validate the schema version matches, and recovery any incomplete flights.
     */
    public Stairway(ExecutorService threadPool, DataSource dataSource, String schemaName, String nameStem, boolean forceCleanStart) {
        this.threadPool = threadPool;
        this.dataSource = dataSource;
        this.schemaName = schemaName;
        this.nameStem = nameStem;
        this.taskContextMap = new ConcurrentHashMap<>();
        this.database = new Database(dataSource, forceCleanStart, schemaName, nameStem);
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
    public String submit(Class<? extends Flight> flightClass, SafeHashMap inputParameters) {
        if (flightClass == null || inputParameters == null) {
            throw new MakeFlightException("Must supply non-null flightClass and inputParameters to submit");
        }
        Flight flight = makeFlight(flightClass, inputParameters);

        // Generate the sequence id as a UUID. We have no dependency on database id generation and no
        // confusion with small integers that might be accidentally valid.
        flight.context().flightId(UUID.randomUUID().toString());

        database.submit(flight.context());

        launchFlight(flight);
        return flight.context().getFlightId();
    }

    // Tests if flight is done
    public boolean isDone(String flightId) {
        TaskContext taskContext = lookupFlight(flightId);
        return taskContext.getFutureResult().isDone();
    }

    /**
     * Wait for a flight to complete and return the results. Once the results have been
     * returned, the flight is removed from the taskContextMap and marked complete in
     * the database.
     *
     * @param flightId
     * @return FlightResult object with status, possible exception, and result parameters
     */
    public FlightResult getResult(String flightId) {
        TaskContext taskContext = lookupFlight(flightId);

        try {
            FlightResult result = taskContext.getFutureResult().get();
            taskContextMap.remove(flightId);
            database.complete(taskContext.flight.context());
            return result;

        } catch (InterruptedException ex) {
            // Someone is shutting down the application
            Thread.currentThread().interrupt();
            return FlightResult.flightResultFatal(ex);
        } catch (ExecutionException ex) {
            return FlightResult.flightResultFatal(ex);
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
     * Find any incomplete flights and recover them. We overwrite the flight context with the recovered
     * flight context. The normal constructor path needs to give the input parameters to the flight
     * subclass. This is a case where we don't really want to have the Flight object set up its own context.
     * It is simpler to override it than to make a separate code path for this recovery case.
     */
    private void recoverFlights() {
        List<FlightContext> flightList = database.recover();
        for (FlightContext flightContext : flightList) {
            Flight flight = makeFlightFromName(flightContext.getFlightClassName(), flightContext.getInputParameters());
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
        TaskContext taskContext = new TaskContext(new FutureTask<FlightResult>(flight), flight);
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
    private Flight makeFlight(Class<? extends Flight> flightClass, SafeHashMap inputParameters) {
        try {
            // Find the flightClass constructor that takes the input parameter map and
            // use it to make the flight.
            Constructor constructor = flightClass.getConstructor(SafeHashMap.class);
            Flight flight = (Flight)constructor.newInstance(inputParameters);
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
    private Flight makeFlightFromName(String className, SafeHashMap inputMap) {
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
        return new ToStringBuilder(this)
                .append("taskContextMap", taskContextMap)
                .append("threadPool", threadPool)
                .append("dataSource", dataSource)
                .append("schemaName", schemaName)
                .append("nameStem", nameStem)
                .append("database", database)
                .toString();
    }
}

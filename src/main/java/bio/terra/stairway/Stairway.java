package bio.terra.stairway;

import bio.terra.controller.AuthenticatedUser;
import bio.terra.exception.UnauthorizedException;
import bio.terra.stairway.exception.DatabaseOperationException;
import bio.terra.stairway.exception.FlightException;
import bio.terra.stairway.exception.FlightNotFoundException;
import bio.terra.stairway.exception.MakeFlightException;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Stairway is the object that drives execution of Flights. The class is constructed
 * with inputs that allow the caller to specify the thread pool, the flightDao source, and the
 * table name stem to use.
 *
 * Each Stairway runs, logs, and recovers independently.
 *
 * There are two techniques you can use to wait for a flight. One is polling by calling getFlightState. That
 * reads the flight state from the stairway flightDao so will report correct state for as long as the flight
 * lives in the flightDao. (Since we haven't implemented pruning, that means forever.) If you poll in this way,
 * then the in-memory resources are released on the first call to getFlightState that reports the flight has
 * completed in some way.
 *
 * The other technique is to poll the flight future using the isDone() method or block waiting for the
 * flight to complete using the waitForFlight() method. In this case, the in-memory resources are freed
 * when either method detects that the flight is done.
 */
public class Stairway {
    private Logger logger = LoggerFactory.getLogger("bio.terra.stairway");

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
    private FlightDao flightDao;
    private Object applicationContext;

    /**
     * We do initialization in two steps. The constructor does the first step of constructing the object
     * and remembering the inputs. It does not do any flightDao activity. That lets the rest of the
     * application come up and do any flightDao configuration.
     *
     * The second step is the 'initialize' call (below) that sets up the flightDao and performs
     * any recovery needed.
     *
     * @param threadPool a thread pool must be provided. The caller chooses the type of pool to use.
     */
    public Stairway(ExecutorService threadPool, Object applicationContext) {
        this.threadPool = threadPool;
        this.applicationContext = applicationContext;
        this.taskContextMap = new ConcurrentHashMap<>();
    }

    /**
     * Second step of initialization
     * @param forceCleanStart true will drop any existing stairway data. Otherwise existing flights are recovered.
     */
    public void initialize(FlightDao flightDao, boolean forceCleanStart) {
        this.flightDao = flightDao;
        if (forceCleanStart) {
            // Clean up if need be
            flightDao.startClean();
        } else {
            recoverFlights();
        }
    }

    public UUID createFlightId() {
        return UUID.randomUUID();
    }

    /**
     * Submit a flight for execution
     *
     * @param flightClass class object of the class derived from Flight; e.g., MyFlight.class
     * @param inputParameters key-value map of parameters to the flight
     * @return unique flight id of the submitted flight
     */
    public void submit(
        String flightId,
        Class<? extends Flight> flightClass,
        FlightMap inputParameters,
        AuthenticatedUser userInfo) {
        if (flightClass == null || inputParameters == null) {
            throw new MakeFlightException("Must supply non-null flightClass and inputParameters to submit");
        }
        Flight flight = makeFlight(flightClass, inputParameters, userInfo);

        // Generate the sequence id as a UUID. We have no dependency on flightDao id generation and no
        // confusion with small integers that might be accidentally valid.
        flight.context().setFlightId(flightId);
        flightDao.submit(flight.context());
        launchFlight(flight);
    }

    public void verifyFlightAccess(String flightId, AuthenticatedUser userInfo) {
        if (userInfo != null) {
            boolean hasAccess = false;
            try {
                hasAccess = flightDao.ownsFlight(flightId, userInfo.getSubjectId());
            } catch (EmptyResultDataAccessException emptyEx) {
                throw new FlightNotFoundException(emptyEx);
            } catch (IncorrectResultSizeDataAccessException multiEx) {
                throw new DatabaseOperationException("Multiple flights with the same id?! " + flightId, multiEx);
            }
            if (!hasAccess) {
                throw new UnauthorizedException("user " + userInfo.getEmail() + " does not own flight " + flightId);
            }
        }
    }



    public void deleteFlight(String flightId) {
        releaseFlight(flightId);
        flightDao.delete(flightId);
    }

    /**
     * Wait for a flight to complete. When it completes, the flight is removed from the taskContextMap.
     * Callers must be sure user has access to flight results
     *
     * @param flightId
     */
    public FlightState waitForFlight(String flightId) {
        TaskContext taskContext = lookupFlight(flightId);

        try {
            FlightState state = taskContext.getFutureResult().get();
            releaseFlight(flightId);
            return state;
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
     * If the flight is complete and still in our in-memory map, we remove it.
     * The logic is that if getFlightState is called, then either the wait
     * finished or we are polling and won't perform a wait.
     *
     * @param flightId
     * @return FlightState
     */
    public FlightState getFlightState(String flightId) {
        FlightState flightState = flightDao.getFlightState(flightId);
        if (flightState.getFlightStatus() != FlightStatus.RUNNING) {
            releaseFlight(flightId);
        }
        return flightState;
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
        return flightDao.getFlights(offset, limit);
    }

    public List<FlightState> getFlightsForUser(int offset, int limit, AuthenticatedUser userReq) {
        return flightDao.getFlightsForUser(offset, limit, userReq.getSubjectId());
    }

    private void releaseFlight(String flightId) {
        TaskContext taskContext = taskContextMap.get(flightId);
        if (taskContext != null) {
            if (!taskContext.getFutureResult().isDone()) {
                logger.warn("Removing flight context for in progress flight " + flightId);
            }
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
     * Find any incomplete flights and recover them. We overwrite the flight context of this flight
     * with the recovered flight context. The normal constructor path needs to give the input parameters
     * to the flight subclass. This is a case where we don't really want to have the Flight object set up
     * its own context. It is simpler to override it than to make a separate code path for this recovery case.
     *
     * The flightlog records the last operation performed; so we need to set the execution point to the next
     * step index.
     */
    private void recoverFlights() {
        List<FlightContext> flightList = flightDao.recover();
        for (FlightContext flightContext : flightList) {
            Flight flight = makeFlightFromName(
                flightContext.getFlightClassName(), flightContext.getInputParameters(), flightContext.getUser());
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
        // Give the flight the flightDao object so it can properly record its steps
        flight.setFlightDao(flightDao);

        // Build the task context to keep track of the running task
        TaskContext taskContext = new TaskContext(new FutureTask<FlightState>(flight), flight);

        if (logger.isDebugEnabled()) {
            if (threadPool instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor tpe = (ThreadPoolExecutor) threadPool;
                logger.debug("Stairway thread pool: " + tpe.getActiveCount() +
                        " active from pool of " + tpe.getPoolSize());
            }
        }
        logger.info("Launching flight " + flight.context().getFlightClassName());

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
    private Flight makeFlight(
        Class<? extends Flight> flightClass, FlightMap inputParameters, AuthenticatedUser userInfo) {
        try {
            // Find the flightClass constructor that takes the input parameter map and
            // use it to make the flight.
            Constructor constructor = flightClass.getConstructor(
                FlightMap.class, Object.class, AuthenticatedUser.class);
            Flight flight = (Flight)constructor.newInstance(inputParameters, applicationContext, userInfo);
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
     * We use the class name to store and retrieve from the flightDao when we recover.
     */
    private Flight makeFlightFromName(String className, FlightMap inputMap, AuthenticatedUser user) {
        try {
            Class<?> someClass = Class.forName(className);
            if (Flight.class.isAssignableFrom(someClass)) {
                Class<? extends Flight> flightClass = (Class<? extends Flight>) someClass;
                return makeFlight(flightClass, inputMap, user);
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
                .append("flightDao", flightDao)
                .append("applicationContext", applicationContext)
                .toString();
    }
}

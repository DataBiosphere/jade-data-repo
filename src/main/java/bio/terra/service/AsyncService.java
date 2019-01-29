package bio.terra.service;

import bio.terra.flight.StudyCreateFlight;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.FlightResult;
import bio.terra.stairway.Stairway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class AsyncService {

    private Stairway stairway;
    private HashMap<String, Class> jobFlights;

    @Autowired
    public AsyncService(AsyncContext asyncContext) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        // TODO: datasource should be wired, and forceCleanStart should be true for local+dev, false o/w
        stairway = new Stairway(executorService, null, false, asyncContext);

        jobFlights = new HashMap<>();
        jobFlights.put("create-study", StudyCreateFlight.class);
    }

    public String submitJob(String jobName, Object inputParams) {
        if (!jobFlights.containsKey(jobName)) {
            throw new RuntimeException("Job name not registered");
        }
        FlightMap flightMap = new FlightMap();
        flightMap.put("request", inputParams);
        String flightId = stairway.submit(jobFlights.get(jobName), flightMap);
        return flightId;
    }

    public <T> T waitForJob(String jobId, Class<? extends T> resultClass) throws AsyncException {
        FlightResult result = stairway.getResult(jobId);
        if (result.isSuccess()) {
            FlightMap resultMap = result.getResultMap();
            return resultMap.get("response", resultClass);
        } else {
            String message = "Could not complete job";
            Optional<Throwable> optThrowable = result.getThrowable();
            if (optThrowable.isPresent()) {
                Throwable throwable = optThrowable.get();
                throwable.printStackTrace();
                throw new AsyncException(message, throwable);
            }
            throw new AsyncException(message);
        }
    }
}

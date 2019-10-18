package bio.terra.stairway;

import bio.terra.app.configuration.StairwayJdbcConfiguration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class TestUtil {
    private TestUtil() {
    }

    static final Integer intValue = Integer.valueOf(22);
    static final String strValue = "testing 1 2 3";
    static final Double dubValue = new Double(Math.PI);
    static final String errString = "Something bad happened";
    static final String flightId = "aaa111";
    static final String ikey = "ikey";
    static final String skey = "skey";
    static final String fkey = "fkey";
    static final String wikey = "wikey";
    static final String wskey = "wskey";
    static final String wfkey = "wfkey";

    static Stairway setupStairway(StairwayJdbcConfiguration jdbcConfiguration) {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Stairway stairway = new Stairway(executorService, null);
        stairway.initialize(new FlightDao(jdbcConfiguration), true);
        return stairway;
    }

    static boolean isDone(Stairway stairway, String flightId) {
        return stairway.getFlightState(flightId).getFlightStatus() != FlightStatus.RUNNING;
    }
}

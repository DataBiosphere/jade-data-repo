package bio.terra.common.fixtures;

import bio.terra.stairway.FlightMap;
import java.util.Map;

public class FlightContextFixtures {

  /**
   * Create a flight context map based on a passed in map. This is helpful for mocking flight step
   * executions.
   */
  public static FlightMap makeContextMap(Map<String, Object> context) {
    FlightMap fMap = new FlightMap();
    context.forEach(fMap::put);
    return fMap;
  }
}

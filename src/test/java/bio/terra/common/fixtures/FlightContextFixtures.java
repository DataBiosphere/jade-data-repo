package bio.terra.common.fixtures;

import bio.terra.stairway.FlightMap;
import java.util.Map;

public class FlightContextFixtures {

  public static FlightMap makeContextMap(Map<String, Object> context) {
    FlightMap fMap = new FlightMap();
    context.forEach(fMap::put);
    return fMap;
  }
}

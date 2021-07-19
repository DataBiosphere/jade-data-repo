package bio.terra.service.filedata.flight.ingest;

import bio.terra.stairway.FlightContext;

public final class IngestUtils {
  private IngestUtils() {}

  /**
   * Given a {@link FlightContext} object, look to see if the there is a value in the input map and
   * if not, read it from the working map
   *
   * @param context The FlightContext object to examine
   * @param key The map key to attempt to read values from
   * @param clazz Class used to deserialize the value from the map
   * @param <T> The type of the expected value in the maps
   * @return A typed value from the flight context with type T or null if no value is found
   */
  public static <T> T getContextValue(FlightContext context, String key, Class<T> clazz) {
    // Typically, when this is used the bucket / storage account has been selected for this file.
    // In the single file load case, the info is stored in the working map. In the bulk load case,
    // the info is stored in the input parameters.
    // TODO: simplify this when we remove single file load
    // TODO: This is a cut and paste from IngestFilePrimaryDataStep. Both can be removed
    //  when we get rid of single file load
    T value = context.getInputParameters().get(key, clazz);
    if (value == null) {
      value = context.getWorkingMap().get(key, clazz);
    }
    return value;
  }
}

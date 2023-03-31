package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.stairway.FlightMap;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class ConvertFileIdUtils {

  private ConvertFileIdUtils() {}

  public static final String FILE_ID_MAPPINGS_FIELD = "flightIdMappings";
  public static final String DATASET_USES_PREDICTABLE_IDS_AT_START =
      "datasetUsesPredictableIdsAtStart";

  public static Map<UUID, UUID> readFlightMappings(FlightMap workingMap) {
    return workingMap
        .get(FILE_ID_MAPPINGS_FIELD, new TypeReference<Map<String, String>>() {})
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(e -> UUID.fromString(e.getKey()), e -> UUID.fromString(e.getValue())));
  }
}

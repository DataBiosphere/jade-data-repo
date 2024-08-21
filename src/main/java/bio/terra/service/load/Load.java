package bio.terra.service.load;

import java.util.Objects;
import java.util.UUID;

public record Load(UUID id, String loadTag, String lockingFlightId, UUID datasetId) {

  /**
   * @return whether this Load is locked by the supplied flight ID and dataset
   */
  public boolean isLockedBy(String lockingFlightId, UUID datasetId) {
    return Objects.equals(this.lockingFlightId, lockingFlightId)
        && Objects.equals(this.datasetId, datasetId);
  }
}

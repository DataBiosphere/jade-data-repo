package bio.terra.service.load;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record Load(
    UUID id,
    String loadTag,
    boolean locked,
    String lockingFlightId,
    UUID datasetId,
    Instant createdDate) {

  /**
   * @return whether this Load is locked by the supplied flight ID and dataset
   */
  public boolean lockedBy(String lockingFlightId, UUID datasetId) {
    return Objects.equals(this.lockingFlightId, lockingFlightId)
        && Objects.equals(this.datasetId, datasetId);
  }
}

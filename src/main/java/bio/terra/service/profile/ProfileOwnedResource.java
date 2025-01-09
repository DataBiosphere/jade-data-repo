package bio.terra.service.profile;

import java.time.Instant;
import java.util.UUID;

public record ProfileOwnedResource(
    UUID id, String name, String description, Instant createdDate, Type type) {
  public enum Type {
    DATASET,
    SNAPSHOT,
  }
}

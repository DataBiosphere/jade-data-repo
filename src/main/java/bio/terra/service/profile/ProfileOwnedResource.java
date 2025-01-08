package bio.terra.service.profile;

import java.util.UUID;

public record ProfileOwnedResource(UUID id, String name, String description, Type type) {
  public enum Type {
    DATASET,
    SNAPSHOT,
  }
}

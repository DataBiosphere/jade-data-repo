package bio.terra.service.profile;

import bio.terra.model.ProfileOwnedResourceModel;
import java.time.Instant;
import java.util.UUID;

public record ProfileOwnedResource(
    UUID id, String name, String description, Instant createdDate, Type type) {
  public enum Type {
    DATASET,
    SNAPSHOT,
  }

  public ProfileOwnedResourceModel toModel() {
    return new ProfileOwnedResourceModel()
        .id(id)
        .name(name)
        .description(description)
        .createdDate(createdDate.toString())
        .type(
            Type.DATASET == type
                ? ProfileOwnedResourceModel.TypeEnum.DATASET
                : ProfileOwnedResourceModel.TypeEnum.SNAPSHOT);
  }
}

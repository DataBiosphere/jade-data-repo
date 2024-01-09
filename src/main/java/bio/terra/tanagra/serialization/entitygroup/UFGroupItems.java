package bio.terra.tanagra.serialization.entitygroup;

import bio.terra.tanagra.serialization.UFEntityGroup;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.entitygroup.GroupItems;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Map;

/**
 * External representation of a group items entity group.
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFGroupItems.Builder.class)
public class UFGroupItems extends UFEntityGroup {
  private final String groupEntity;
  private final String itemsEntity;

  public UFGroupItems(GroupItems groupItems) {
    super(groupItems);
    this.groupEntity = groupItems.getGroupEntity().getName();
    this.itemsEntity = groupItems.getItemsEntity().getName();
  }

  private UFGroupItems(Builder builder) {
    super(builder);
    this.groupEntity = builder.groupEntity;
    this.itemsEntity = builder.itemsEntity;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder extends UFEntityGroup.Builder {
    private String groupEntity;
    private String itemsEntity;

    public Builder groupEntity(String groupEntity) {
      this.groupEntity = groupEntity;
      return this;
    }

    public Builder itemsEntity(String itemsEntity) {
      this.itemsEntity = itemsEntity;
      return this;
    }

    @Override
    public UFGroupItems build() {
      return new UFGroupItems(this);
    }
  }

  @Override
  public GroupItems deserializeToInternal(
      Map<String, DataPointer> dataPointers,
      Map<String, Entity> entities,
      String primaryEntityName) {
    return GroupItems.fromSerialized(this, dataPointers, entities);
  }

  public String getGroupEntity() {
    return groupEntity;
  }

  public String getItemsEntity() {
    return itemsEntity;
  }
}

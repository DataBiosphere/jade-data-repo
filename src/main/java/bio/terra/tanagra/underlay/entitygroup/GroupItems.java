package bio.terra.tanagra.underlay.entitygroup;

import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.EntityGroup;
import bio.terra.tanagra.underlay.EntityGroupMapping;
import bio.terra.tanagra.underlay.Relationship;
import java.util.Map;

public class GroupItems extends EntityGroup {
  private static final String GROUP_ENTITY_NAME = "group";
  private static final String ITEMS_ENTITY_NAME = "items";
  private static final String GROUP_ITEMS_RELATIONSHIP_NAME = "groupToItems";

  private final Entity groupEntity;
  private final Entity itemsEntity;

  private GroupItems(String name, Map<String, Relationship> relationships, EntityGroupMapping sourceDataMapping, EntityGroupMapping indexDataMapping, Entity groupEntity, Entity itemsEntity) {
    super(name, relationships, sourceDataMapping, indexDataMapping);
    this.groupEntity = groupEntity;
    this.itemsEntity = itemsEntity;
  }

  @Override
  public Type getType() {
    return Type.GROUP_ITEMS;
  }

  public Entity getGroupEntity() {
    return groupEntity;
  }

  public Entity getItemsEntity() {
    return itemsEntity;
  }

  @Override
  public Map<String, Entity> getEntityMap() {
    return Map.of(GROUP_ENTITY_NAME, groupEntity, ITEMS_ENTITY_NAME, itemsEntity);
  }

  public static class Builder extends EntityGroup.Builder {
    private Entity groupEntity;
    private Entity itemsEntity;

    public Builder groupEntity(Entity groupEntity) {
      this.groupEntity = groupEntity;
      return this;
    }

    public Builder itemsEntity(Entity itemsEntity) {
      this.itemsEntity = itemsEntity;
      return this;
    }

    @Override
    public GroupItems build() {
      return new GroupItems(name, relationships, sourceDataMapping, indexDataMapping, groupEntity, itemsEntity);
    }
  }
}

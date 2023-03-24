package bio.terra.service.cohortbuilder.tanagra.instances;

import bio.terra.tanagra.query.OrderByDirection;
import bio.terra.tanagra.underlay.Attribute;
import bio.terra.tanagra.underlay.RelationshipField;

public class EntityQueryOrderBy {
  private final Attribute attribute;
  private final RelationshipField relationshipField;
  private final OrderByDirection direction;

  public EntityQueryOrderBy(Attribute attribute, OrderByDirection direction) {
    this.attribute = attribute;
    this.relationshipField = null;
    this.direction = direction;
  }

  public EntityQueryOrderBy(RelationshipField relationshipField, OrderByDirection direction) {
    this.attribute = null;
    this.relationshipField = relationshipField;
    this.direction = direction;
  }

  public Attribute getAttribute() {
    return attribute;
  }

  public RelationshipField getRelationshipField() {
    return relationshipField;
  }

  public OrderByDirection getDirection() {
    return direction;
  }

  public boolean isByAttribute() {
    return attribute != null;
  }
}

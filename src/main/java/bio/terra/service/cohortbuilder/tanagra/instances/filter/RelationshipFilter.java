package bio.terra.service.cohortbuilder.tanagra.instances.filter;

import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.filtervariable.SubQueryFilterVariable;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Relationship;
import bio.terra.tanagra.underlay.RelationshipMapping;
import bio.terra.tanagra.underlay.Underlay;
import com.google.common.collect.Lists;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationshipFilter extends EntityFilter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelationshipFilter.class);

  private final Entity selectEntity;
  private final Entity filterEntity;
  private final Relationship relationship;
  private final EntityFilter subFilter;

  public RelationshipFilter(
      Entity selectEntity, Relationship relationship, EntityFilter subFilter) {
    this.selectEntity = selectEntity;
    this.filterEntity =
        relationship.getEntityA().equals(selectEntity)
            ? relationship.getEntityB()
            : relationship.getEntityA();
    this.relationship = relationship;
    this.subFilter = subFilter;
  }

  @Override
  public FilterVariable getFilterVariable(
      TableVariable entityTableVar, List<TableVariable> tableVars) {
    FieldPointer entityIdFieldPointer =
        selectEntity.getIdAttribute().getMapping(Underlay.MappingType.INDEX).getValue();

    // build a query to get all related entity instance ids:
    //   SELECT relatedEntityId FROM relatedEntityTable WHERE subFilter
    TableVariable relatedEntityTableVar =
        TableVariable.forPrimary(
            filterEntity.getMapping(Underlay.MappingType.INDEX).getTablePointer());
    List<TableVariable> relatedEntityTableVars = Lists.newArrayList(relatedEntityTableVar);

    FieldVariable relatedEntityIdFieldVar =
        filterEntity
            .getIdAttribute()
            .getMapping(Underlay.MappingType.INDEX)
            .getValue()
            .buildVariable(relatedEntityTableVar, relatedEntityTableVars);
    FilterVariable relatedEntityFilterVar =
        subFilter.getFilterVariable(relatedEntityTableVar, relatedEntityTableVars);

    Query relatedEntityQuery =
        new Query.Builder()
            .select(List.of(relatedEntityIdFieldVar))
            .tables(relatedEntityTableVars)
            .where(relatedEntityFilterVar)
            .build();
    LOGGER.info("Generated query: {}", relatedEntityQuery.renderSQL());

    RelationshipMapping indexMapping = relationship.getMapping(Underlay.MappingType.INDEX);
    if (indexMapping
        .getIdPairsTable()
        .equals(selectEntity.getMapping(Underlay.MappingType.INDEX).getTablePointer())) {
      LOGGER.info("Relationship table is the same as the entity table");
      // build a filter variable for the entity table on the sub query
      //  WHERE relatedEntityId IN (SELECT relatedEntityId FROM relatedEntityTable WHERE subFilter)
      FieldVariable filterEntityIdFieldVar =
          indexMapping.getIdPairsId(filterEntity).buildVariable(entityTableVar, tableVars);
      return new SubQueryFilterVariable(
          filterEntityIdFieldVar, SubQueryFilterVariable.Operator.IN, relatedEntityQuery);
    } else {
      LOGGER.info("Relationship table is different from the entity table");
      // build another query to get all entity instance ids from the relationship table:
      //  SELECT fromEntityId FROM relationshipTable WHERE
      //  toEntityId IN (SELECT relatedEntityId FROM relatedEntityTable WHERE subFilter)
      TableVariable relationshipTableVar = TableVariable.forPrimary(indexMapping.getIdPairsTable());
      List<TableVariable> relationshipTableVars = Lists.newArrayList(relationshipTableVar);

      FieldVariable selectEntityIdFieldVar =
          indexMapping
              .getIdPairsId(selectEntity)
              .buildVariable(relationshipTableVar, relationshipTableVars);
      FieldVariable filterEntityIdFieldVar =
          indexMapping
              .getIdPairsId(filterEntity)
              .buildVariable(relationshipTableVar, relationshipTableVars);
      SubQueryFilterVariable relationshipFilterVar =
          new SubQueryFilterVariable(
              filterEntityIdFieldVar, SubQueryFilterVariable.Operator.IN, relatedEntityQuery);

      Query relationshipQuery =
          new Query.Builder()
              .select(List.of(selectEntityIdFieldVar))
              .tables(relationshipTableVars)
              .where(relationshipFilterVar)
              .build();
      LOGGER.info("Generated query: {}", relatedEntityQuery.renderSQL());

      // build a filter variable for the entity table on the sub query
      //  WHERE entityId IN (SELECT fromEntityId FROM relationshipTable WHERE
      //  toEntityId IN (SELECT relatedEntityId FROM relatedEntityTable WHERE subFilter))
      FieldVariable entityIdFieldVar =
          entityIdFieldPointer.buildVariable(entityTableVar, tableVars);
      return new SubQueryFilterVariable(
          entityIdFieldVar, SubQueryFilterVariable.Operator.IN, relationshipQuery);
    }
  }
}

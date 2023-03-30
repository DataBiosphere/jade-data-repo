package bio.terra.service.cohortbuilder;

import bio.terra.model.CountQuery;
import bio.terra.model.Instance;
import bio.terra.model.InstanceCountList;
import bio.terra.model.InstanceHierarchyFields;
import bio.terra.model.InstanceList;
import bio.terra.model.InstanceRelationshipFields;
import bio.terra.model.Query;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.OrderByDirection;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.service.FromApiConversionService;
import bio.terra.tanagra.service.QuerysService;
import bio.terra.tanagra.service.UnderlaysService;
import bio.terra.tanagra.service.instances.EntityInstance;
import bio.terra.tanagra.service.instances.EntityInstanceCount;
import bio.terra.tanagra.service.instances.EntityQueryOrderBy;
import bio.terra.tanagra.service.instances.EntityQueryRequest;
import bio.terra.tanagra.service.instances.filter.EntityFilter;
import bio.terra.tanagra.service.utils.ToApiConversionUtils;
import bio.terra.tanagra.service.utils.ValidationUtils;
import bio.terra.tanagra.underlay.Attribute;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Hierarchy;
import bio.terra.tanagra.underlay.HierarchyField;
import bio.terra.tanagra.underlay.Relationship;
import bio.terra.tanagra.underlay.RelationshipField;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.ValueDisplay;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class InstancesService {
  private final UnderlaysService underlaysService;
  private final QuerysService querysService;
  private final FromApiConversionService fromApiConversionService;

  @Autowired
  public InstancesService(
      UnderlaysService underlaysService,
      QuerysService querysService,
      FromApiConversionService fromApiConversionService) {
    this.underlaysService = underlaysService;
    this.querysService = querysService;
    this.fromApiConversionService = fromApiConversionService;
  }

  public InstanceCountList countInstances(String entityName, CountQuery countQuery) {
    bio.terra.tanagra.underlay.Entity entity = underlaysService.getEntity(entityName);

    List<Attribute> attributes = new ArrayList<>();
    if (countQuery.getAttributes() != null) {
      attributes =
          countQuery.getAttributes().stream()
              .map(attrName -> querysService.getAttribute(entity, attrName))
              .collect(Collectors.toList());
    }

    EntityFilter entityFilter = null;
    if (countQuery.getFilter() != null) {
      entityFilter = fromApiConversionService.fromApiObject(countQuery.getFilter(), entity);
    }

    bio.terra.tanagra.query.QueryRequest queryRequest =
        querysService.buildInstanceCountsQuery(
            entity, Underlay.MappingType.INDEX, attributes, entityFilter);
    List<EntityInstanceCount> entityInstanceCounts =
        querysService.runInstanceCountsQuery(
            entity.getMapping(Underlay.MappingType.INDEX).getTablePointer().getDataPointer(),
            attributes,
            queryRequest);

    return new InstanceCountList()
        .instanceCounts(
            entityInstanceCounts.stream()
                .map(ToApiConversionUtils::toApiObject)
                .collect(Collectors.toList()))
        .sql(queryRequest.query().renderSQL());
  }

  public InstanceList queryInstances(String entityName, Query query) {
    ValidationUtils.validateApiFilter(query.getFilter());

    Entity entity = underlaysService.getEntity(entityName);
    List<Attribute> selectAttributes = selectAttributesFromRequest(query, entity);
    List<HierarchyField> selectHierarchyFields = selectHierarchyFieldsFromRequest(query, entity);
    List<RelationshipField> selectRelationshipFields =
        selectRelationshipFieldsFromRequest(query, entity);
    List<EntityQueryOrderBy> entityOrderBys = entityOrderBysFromRequest(query, entity);
    EntityFilter entityFilter = null;
    if (query.getFilter() != null) {
      entityFilter = fromApiConversionService.fromApiObject(query.getFilter(), entity);
    }

    QueryRequest queryRequest =
        querysService.buildInstancesQuery(
            new EntityQueryRequest.Builder()
                .entity(entity)
                .mappingType(Underlay.MappingType.INDEX)
                .selectAttributes(selectAttributes)
                .selectHierarchyFields(selectHierarchyFields)
                .selectRelationshipFields(selectRelationshipFields)
                .filter(entityFilter)
                .orderBys(entityOrderBys)
                .limit(query.getLimit())
                .build());
    DataPointer indexDataPointer =
        entity.getMapping(Underlay.MappingType.INDEX).getTablePointer().getDataPointer();
    List<EntityInstance> entityInstances =
        querysService.runInstancesQuery(
            indexDataPointer,
            selectAttributes,
            selectHierarchyFields,
            selectRelationshipFields,
            queryRequest);

    return new InstanceList()
        .instances(entityInstances.stream().map(this::toApiObject).collect(Collectors.toList()))
        .sql(queryRequest.query().renderSQL());
  }

  private List<Attribute> selectAttributesFromRequest(Query body, Entity entity) {
    List<Attribute> selectAttributes = new ArrayList<>();
    if (body.getIncludeAttributes() != null) {
      selectAttributes =
          body.getIncludeAttributes().stream()
              .map(attrName -> querysService.getAttribute(entity, attrName))
              .collect(Collectors.toList());
    }
    return selectAttributes;
  }

  private static List<HierarchyField> selectHierarchyFieldsFromRequest(Query query, Entity entity) {
    List<HierarchyField> selectHierarchyFields = new ArrayList<>();
    if (query.getIncludeHierarchyFields() != null) {
      // for each hierarchy, return all the fields specified
      query
          .getIncludeHierarchyFields()
          .getHierarchies()
          .forEach(
              hierarchyName -> {
                Hierarchy hierarchy = entity.getHierarchy(hierarchyName);
                query
                    .getIncludeHierarchyFields()
                    .getFields()
                    .forEach(
                        hierarchyFieldName ->
                            selectHierarchyFields.add(
                                hierarchy.getField(
                                    HierarchyField.Type.valueOf(hierarchyFieldName.name()))));
              });
    }
    return selectHierarchyFields;
  }

  private List<RelationshipField> selectRelationshipFieldsFromRequest(Query query, Entity entity) {
    List<RelationshipField> selectRelationshipFields = new ArrayList<>();
    if (query.getIncludeRelationshipFields() != null) {
      // for each related entity, return all the fields specified
      query
          .getIncludeRelationshipFields()
          .forEach(
              includeRelationshipField -> {
                Entity relatedEntity =
                    underlaysService.getEntity(includeRelationshipField.getRelatedEntity());
                List<Hierarchy> hierarchies = new ArrayList<>();
                hierarchies.add(null); // Always return the NO_HIERARCHY rollups.
                if (includeRelationshipField.getHierarchies() != null
                    && !includeRelationshipField.getHierarchies().isEmpty()) {
                  includeRelationshipField
                      .getHierarchies()
                      .forEach(
                          hierarchyName -> hierarchies.add(entity.getHierarchy(hierarchyName)));
                }

                hierarchies.forEach(
                    hierarchy -> {
                      Relationship relationship = entity.getRelationship(relatedEntity);
                      selectRelationshipFields.add(
                          relationship.getField(RelationshipField.Type.COUNT, entity, hierarchy));
                    });
              });
    }
    return selectRelationshipFields;
  }

  private List<EntityQueryOrderBy> entityOrderBysFromRequest(Query query, Entity entity) {
    List<EntityQueryOrderBy> entityOrderBys = new ArrayList<>();
    if (query.getOrderBys() != null) {
      query
          .getOrderBys()
          .forEach(
              orderBy -> {
                OrderByDirection direction =
                    orderBy.getDirection() == null
                        ? OrderByDirection.ASCENDING
                        : OrderByDirection.valueOf(orderBy.getDirection().name());
                String attrName = orderBy.getAttribute();
                if (attrName != null) {
                  entityOrderBys.add(
                      new EntityQueryOrderBy(
                          querysService.getAttribute(entity, attrName), direction));
                } else {
                  Entity relatedEntity =
                      underlaysService.getEntity(orderBy.getRelationshipField().getRelatedEntity());
                  Relationship relationship = entity.getRelationship(relatedEntity);

                  String hierName = orderBy.getRelationshipField().getHierarchy();
                  Hierarchy hierarchy = hierName == null ? null : entity.getHierarchy(hierName);
                  entityOrderBys.add(
                      new EntityQueryOrderBy(
                          relationship.getField(RelationshipField.Type.COUNT, entity, hierarchy),
                          direction));
                }
              });
    }
    return entityOrderBys;
  }

  private Instance toApiObject(EntityInstance entityInstance) {
    Instance instance = new Instance();
    Map<String, bio.terra.model.ValueDisplay> attributes = new HashMap<>();
    for (Map.Entry<Attribute, bio.terra.tanagra.underlay.ValueDisplay> attributeValue :
        entityInstance.getAttributeValues().entrySet()) {
      attributes.put(
          attributeValue.getKey().getName(),
          ToApiConversionUtils.toApiObject(attributeValue.getValue()));
    }

    Map<String, InstanceHierarchyFields> hierarchyFieldSets = new HashMap<>();
    for (Map.Entry<HierarchyField, bio.terra.tanagra.underlay.ValueDisplay> hierarchyFieldValue :
        entityInstance.getHierarchyFieldValues().entrySet()) {
      HierarchyField hierarchyField = hierarchyFieldValue.getKey();
      ValueDisplay valueDisplay = hierarchyFieldValue.getValue();

      InstanceHierarchyFields hierarchyFieldSet =
          hierarchyFieldSets.get(hierarchyField.getHierarchy().getName());
      if (hierarchyFieldSet == null) {
        hierarchyFieldSet =
            new InstanceHierarchyFields().hierarchy(hierarchyField.getHierarchy().getName());
        hierarchyFieldSets.put(hierarchyField.getHierarchy().getName(), hierarchyFieldSet);
      }
      switch (hierarchyField.getType()) {
        case IS_MEMBER -> hierarchyFieldSet.isMember(valueDisplay.getValue().getBooleanVal());
        case IS_ROOT -> hierarchyFieldSet.isRoot(valueDisplay.getValue().getBooleanVal());
        case PATH -> hierarchyFieldSet.path(valueDisplay.getValue().getStringVal());
        case NUM_CHILDREN -> hierarchyFieldSet.numChildren(
            Math.toIntExact(valueDisplay.getValue().getInt64Val()));
        default -> throw new SystemException(
            "Unknown hierarchy field type: " + hierarchyField.getType());
      }
    }

    Map<String, InstanceRelationshipFields> relationshipFieldSets = new HashMap<>();
    for (Map.Entry<RelationshipField, ValueDisplay> relationshipFieldValue :
        entityInstance.getRelationshipFieldValues().entrySet()) {
      RelationshipField relationshipField = relationshipFieldValue.getKey();
      ValueDisplay valueDisplay = relationshipFieldValue.getValue();

      InstanceRelationshipFields relationshipFieldSet =
          relationshipFieldSets.get(relationshipField.getName());
      if (relationshipFieldSet == null) {
        relationshipFieldSet =
            new InstanceRelationshipFields()
                .relatedEntity(
                    relationshipField
                        .getRelationship()
                        .getRelatedEntity(relationshipField.getEntity())
                        .getName())
                .hierarchy(
                    relationshipField.getHierarchy() == null
                        ? null
                        : relationshipField.getHierarchy().getName());
        relationshipFieldSets.put(relationshipField.getName(), relationshipFieldSet);
      }
      switch (relationshipField.getType()) {
        case COUNT -> relationshipFieldSet.count(
            Math.toIntExact(valueDisplay.getValue().getInt64Val()));
        case DISPLAY_HINTS -> relationshipFieldSet.displayHints(
            valueDisplay.getValue().getStringVal());
        default -> throw new SystemException(
            "Unknown relationship field type: " + relationshipField.getType());
      }
    }

    return instance
        .attributes(attributes)
        .hierarchyFields(List.copyOf(hierarchyFieldSets.values()))
        .relationshipFields(List.copyOf(relationshipFieldSets.values()));
  }
}

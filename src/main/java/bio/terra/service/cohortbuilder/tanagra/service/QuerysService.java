package bio.terra.service.cohortbuilder.tanagra.service;

import static bio.terra.service.cohortbuilder.tanagra.instances.EntityInstanceCount.DEFAULT_COUNT_COLUMN_NAME;
import static bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence.MODIFIER_AUX_DATA_ATTR_COL;
import static bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence.MODIFIER_AUX_DATA_ENUM_COUNT_COL;
import static bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence.MODIFIER_AUX_DATA_ENUM_DISPLAY_COL;
import static bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence.MODIFIER_AUX_DATA_ENUM_VAL_COL;
import static bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence.MODIFIER_AUX_DATA_ID_COL;
import static bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence.MODIFIER_AUX_DATA_MAX_COL;
import static bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence.MODIFIER_AUX_DATA_MIN_COL;

import bio.terra.common.exception.NotFoundException;
import bio.terra.service.cohortbuilder.tanagra.configuration.TanagraExportConfiguration;
import bio.terra.service.cohortbuilder.tanagra.instances.EntityInstance;
import bio.terra.service.cohortbuilder.tanagra.instances.EntityInstanceCount;
import bio.terra.service.cohortbuilder.tanagra.instances.EntityQueryRequest;
import bio.terra.service.cohortbuilder.tanagra.instances.filter.EntityFilter;
import bio.terra.tanagra.exception.InvalidQueryException;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnHeaderSchema;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.OrderByVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.query.QueryResult;
import bio.terra.tanagra.query.RowResult;
import bio.terra.tanagra.query.TableVariable;
import bio.terra.tanagra.query.filtervariable.BinaryFilterVariable;
import bio.terra.tanagra.underlay.Attribute;
import bio.terra.tanagra.underlay.AttributeMapping;
import bio.terra.tanagra.underlay.AuxiliaryDataMapping;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.DisplayHint;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.EntityGroup;
import bio.terra.tanagra.underlay.EntityMapping;
import bio.terra.tanagra.underlay.Hierarchy;
import bio.terra.tanagra.underlay.HierarchyField;
import bio.terra.tanagra.underlay.HierarchyMapping;
import bio.terra.tanagra.underlay.Relationship;
import bio.terra.tanagra.underlay.RelationshipField;
import bio.terra.tanagra.underlay.RelationshipMapping;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.ValueDisplay;
import bio.terra.tanagra.underlay.displayhint.EnumVal;
import bio.terra.tanagra.underlay.displayhint.EnumVals;
import bio.terra.tanagra.underlay.displayhint.NumericRange;
import bio.terra.tanagra.underlay.entitygroup.CriteriaOccurrence;
import bio.terra.tanagra.utils.GcsUtils;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class QuerysService {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuerysService.class);

  private final TanagraExportConfiguration tanagraExportConfiguration;

  @Autowired
  public QuerysService(TanagraExportConfiguration tanagraExportConfiguration) {
    this.tanagraExportConfiguration = tanagraExportConfiguration;
  }

  public QueryRequest buildInstancesQuery(EntityQueryRequest entityQueryRequest) {
    EntityMapping entityMapping =
        entityQueryRequest.getEntity().getMapping(entityQueryRequest.getMappingType());
    TableVariable entityTableVar = TableVariable.forPrimary(entityMapping.getTablePointer());
    List<TableVariable> tableVars = Lists.newArrayList(entityTableVar);

    // build the SELECT field variables and column schemas from attributes
    List<FieldVariable> selectFieldVars = new ArrayList<>();
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    entityQueryRequest.getSelectAttributes().stream()
        .forEach(
            attribute -> {
              AttributeMapping attributeMapping =
                  attribute.getMapping(entityQueryRequest.getMappingType());
              selectFieldVars.addAll(
                  attributeMapping.buildFieldVariables(entityTableVar, tableVars));
              columnSchemas.addAll(attributeMapping.buildColumnSchemas());
            });

    // build the additional SELECT field variables and column schemas from hierarchy fields
    entityQueryRequest.getSelectHierarchyFields().stream()
        .forEach(
            hierarchyField -> {
              HierarchyMapping hierarchyMapping =
                  entityQueryRequest
                      .getEntity()
                      .getHierarchy(hierarchyField.getHierarchy().getName())
                      .getMapping(entityQueryRequest.getMappingType());
              selectFieldVars.add(
                  hierarchyField.buildFieldVariableFromEntityId(
                      hierarchyMapping, entityTableVar, tableVars));
              columnSchemas.add(hierarchyField.buildColumnSchema());
            });

    // build the additional SELECT field variables and column schemas from relationship fields
    entityQueryRequest.getSelectRelationshipFields().stream()
        .forEach(
            relationshipField -> {
              RelationshipMapping relationshipMapping =
                  relationshipField
                      .getRelationship()
                      .getMapping(entityQueryRequest.getMappingType());
              selectFieldVars.add(
                  relationshipField.buildFieldVariableFromEntityId(
                      relationshipMapping, entityTableVar, tableVars));
              columnSchemas.add(relationshipField.buildColumnSchema());
            });

    // build the ORDER BY field variables from attributes and relationship fields
    List<OrderByVariable> orderByVars =
        entityQueryRequest.getOrderBys().stream()
            .map(
                entityOrderBy -> {
                  FieldVariable fieldVar;
                  if (entityOrderBy.isByAttribute()) {
                    fieldVar =
                        entityOrderBy
                            .getAttribute()
                            .getMapping(entityQueryRequest.getMappingType())
                            .getValue()
                            .buildVariable(entityTableVar, tableVars);
                  } else {
                    RelationshipMapping relationshipMapping =
                        entityOrderBy
                            .getRelationshipField()
                            .getRelationship()
                            .getMapping(entityQueryRequest.getMappingType());
                    fieldVar =
                        entityOrderBy
                            .getRelationshipField()
                            .buildFieldVariableFromEntityId(
                                relationshipMapping, entityTableVar, tableVars);
                  }
                  return new OrderByVariable(fieldVar, entityOrderBy.getDirection());
                })
            .collect(Collectors.toList());

    // build the WHERE filter variables from the entity filter
    FilterVariable filterVar =
        entityQueryRequest.getFilter() == null
            ? null
            : entityQueryRequest.getFilter().getFilterVariable(entityTableVar, tableVars);

    Query query =
        new Query.Builder()
            .select(selectFieldVars)
            .tables(tableVars)
            .where(filterVar)
            .orderBy(orderByVars)
            .limit(entityQueryRequest.getLimit())
            .build();
    LOGGER.info("Generated query: {}", query.renderSQL());

    return new QueryRequest(query.renderSQL(), new ColumnHeaderSchema(columnSchemas));
  }

  public List<EntityInstance> runInstancesQuery(
      DataPointer dataPointer,
      List<Attribute> selectAttributes,
      List<HierarchyField> selectHierarchyFields,
      List<RelationshipField> selectRelationshipFields,
      QueryRequest queryRequest) {
    QueryResult queryResult = dataPointer.getQueryExecutor().execute(queryRequest);

    List<EntityInstance> instances = new ArrayList<>();
    Iterator<RowResult> rowResultsItr = queryResult.getRowResults().iterator();
    while (rowResultsItr.hasNext()) {
      instances.add(
          EntityInstance.fromRowResult(
              rowResultsItr.next(),
              selectAttributes,
              selectHierarchyFields,
              selectRelationshipFields));
    }
    return instances;
  }

  /**
   * @return GCS signed URL of GCS file containing dataset CSV
   */
  public String runInstancesQueryAndExportResultsToGcs(
      DataPointer dataPointer, QueryRequest queryRequest) {
    String gcsBucketProjectId = tanagraExportConfiguration.getGcsBucketProjectId();
    String gcsBucketName = tanagraExportConfiguration.getGcsBucketName();

    if (StringUtils.isEmpty(gcsBucketProjectId) || StringUtils.isEmpty(gcsBucketName)) {
      throw new SystemException(
          "For export, gcsBucketProjectId and gcsBucketName properties must be set");
    }

    String fileName =
        dataPointer.getQueryExecutor().executeAndExportResultsToGcs(queryRequest, gcsBucketName);
    return GcsUtils.createSignedUrl(gcsBucketProjectId, gcsBucketName, fileName);
  }

  public QueryRequest buildInstanceCountsQuery(
      Entity entity,
      Underlay.MappingType mappingType,
      List<Attribute> attributes,
      @Nullable EntityFilter filter) {
    TableVariable entityTableVar =
        TableVariable.forPrimary(entity.getMapping(mappingType).getTablePointer());
    List<TableVariable> tableVars = Lists.newArrayList(entityTableVar);

    // Use the same attributes for SELECT, GROUP BY, and ORDER BY.
    // Build the field variables and column schemas from attributes.
    List<FieldVariable> attributeFieldVars = new ArrayList<>();
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    attributes.stream()
        .forEach(
            attribute -> {
              AttributeMapping attributeMapping = attribute.getMapping(Underlay.MappingType.INDEX);
              attributeFieldVars.addAll(
                  attributeMapping.buildFieldVariables(entityTableVar, tableVars));
              columnSchemas.addAll(attributeMapping.buildColumnSchemas());
            });

    // Additionally, build a count field variable and column schema to SELECT.
    List<FieldVariable> selectFieldVars = new ArrayList<>(attributeFieldVars);
    FieldPointer entityIdFieldPointer =
        entity.getIdAttribute().getMapping(Underlay.MappingType.INDEX).getValue();
    FieldPointer countFieldPointer =
        new FieldPointer.Builder()
            .tablePointer(entityIdFieldPointer.getTablePointer())
            .columnName(entityIdFieldPointer.getColumnName())
            .sqlFunctionWrapper("COUNT")
            .build();
    selectFieldVars.add(
        countFieldPointer.buildVariable(entityTableVar, tableVars, DEFAULT_COUNT_COLUMN_NAME));
    columnSchemas.add(new ColumnSchema(DEFAULT_COUNT_COLUMN_NAME, CellValue.SQLDataType.INT64));

    // Build the WHERE filter variables from the entity filter.
    FilterVariable filterVar =
        filter == null ? null : filter.getFilterVariable(entityTableVar, tableVars);

    // Build the ORDER BY variables using the default direction.
    List<OrderByVariable> orderByVars =
        attributeFieldVars.stream()
            .map(attrFv -> new OrderByVariable(attrFv))
            .collect(Collectors.toList());

    Query query =
        new Query.Builder()
            .select(selectFieldVars)
            .tables(tableVars)
            .where(filterVar)
            .groupBy(attributeFieldVars)
            .orderBy(orderByVars)
            .build();
    LOGGER.info("Generated query: {}", query.renderSQL());

    return new QueryRequest(query.renderSQL(), new ColumnHeaderSchema(columnSchemas));
  }

  public List<EntityInstanceCount> runInstanceCountsQuery(
      DataPointer dataPointer, List<Attribute> attributes, QueryRequest queryRequest) {
    QueryResult queryResult = dataPointer.getQueryExecutor().execute(queryRequest);

    List<EntityInstanceCount> instanceCounts = new ArrayList<>();
    Iterator<RowResult> rowResultsItr = queryResult.getRowResults().iterator();
    while (rowResultsItr.hasNext()) {
      instanceCounts.add(EntityInstanceCount.fromRowResult(rowResultsItr.next(), attributes));
    }
    return instanceCounts;
  }

  public QueryRequest buildDisplayHintsQuery(
      Underlay underlay,
      Entity entity,
      Underlay.MappingType mappingType,
      Entity relatedEntity,
      Literal relatedEntityId) {
    // TODO: Support display hints for any relationship, not just for the CRITERIA_OCCURRENCE entity
    // group.
    EntityGroup entityGroup =
        getRelationship(underlay.getEntityGroups().values(), entity, relatedEntity)
            .getEntityGroup();
    if (!(entityGroup instanceof CriteriaOccurrence)) {
      throw new InvalidQueryException(
          "Only CRITERIA_OCCURENCE entity groups support display hints queries");
    }
    CriteriaOccurrence criteriaOccurrence = (CriteriaOccurrence) entityGroup;
    AuxiliaryDataMapping auxDataMapping =
        criteriaOccurrence.getModifierAuxiliaryData().getMapping(mappingType);

    TableVariable primaryTableVar = TableVariable.forPrimary(auxDataMapping.getTablePointer());
    List<TableVariable> tableVars = Lists.newArrayList(primaryTableVar);

    List<FieldVariable> selectFieldVars =
        auxDataMapping.getFieldPointers().entrySet().stream()
            .map(
                entry -> entry.getValue().buildVariable(primaryTableVar, tableVars, entry.getKey()))
            .collect(Collectors.toList());
    // TODO: Centralize this schema definition used both here and in the indexing job. Also handle
    // other enum_value data types.
    List<ColumnSchema> columnSchemas =
        List.of(
            new ColumnSchema(MODIFIER_AUX_DATA_ID_COL, CellValue.SQLDataType.INT64),
            new ColumnSchema(MODIFIER_AUX_DATA_ATTR_COL, CellValue.SQLDataType.STRING),
            new ColumnSchema(MODIFIER_AUX_DATA_MIN_COL, CellValue.SQLDataType.FLOAT),
            new ColumnSchema(MODIFIER_AUX_DATA_MAX_COL, CellValue.SQLDataType.FLOAT),
            new ColumnSchema(MODIFIER_AUX_DATA_ENUM_VAL_COL, CellValue.SQLDataType.INT64),
            new ColumnSchema(MODIFIER_AUX_DATA_ENUM_DISPLAY_COL, CellValue.SQLDataType.STRING),
            new ColumnSchema(MODIFIER_AUX_DATA_ENUM_COUNT_COL, CellValue.SQLDataType.INT64));

    // Build the WHERE filter variables from the entity filter.
    FieldVariable entityIdFieldVar =
        selectFieldVars.stream()
            .filter(fv -> fv.getAlias().equals(MODIFIER_AUX_DATA_ID_COL))
            .findFirst()
            .get();
    BinaryFilterVariable filterVar =
        new BinaryFilterVariable(
            entityIdFieldVar, BinaryFilterVariable.BinaryOperator.EQUALS, relatedEntityId);

    Query query =
        new Query.Builder().select(selectFieldVars).tables(tableVars).where(filterVar).build();
    LOGGER.info("Generated display hint query: {}", query.renderSQL());

    return new QueryRequest(query.renderSQL(), new ColumnHeaderSchema(columnSchemas));
  }

  public Map<String, DisplayHint> runDisplayHintsQuery(
      DataPointer dataPointer, QueryRequest queryRequest) {
    QueryResult queryResult = dataPointer.getQueryExecutor().execute(queryRequest);

    Map<String, DisplayHint> displayHints = new HashMap<>();
    Map<String, List<EnumVal>> runningEnumValsMap = new HashMap<>();
    Iterator<RowResult> rowResultsItr = queryResult.getRowResults().iterator();
    while (rowResultsItr.hasNext()) {
      RowResult rowResult = rowResultsItr.next();
      String attrName =
          rowResult.get(MODIFIER_AUX_DATA_ATTR_COL).getLiteral().orElseThrow().getStringVal();

      OptionalDouble min = rowResult.get(MODIFIER_AUX_DATA_MIN_COL).getDouble();
      if (min.isPresent()) {
        // This is a numeric range hint.
        OptionalDouble max = rowResult.get(MODIFIER_AUX_DATA_MAX_COL).getDouble();
        displayHints.put(attrName, new NumericRange(min.getAsDouble(), max.getAsDouble()));
      } else {
        // This is an enum values hint.
        Literal val = rowResult.get(MODIFIER_AUX_DATA_ENUM_VAL_COL).getLiteral().orElseThrow();
        Optional<String> display = rowResult.get(MODIFIER_AUX_DATA_ENUM_DISPLAY_COL).getString();
        OptionalLong count = rowResult.get(MODIFIER_AUX_DATA_ENUM_COUNT_COL).getLong();
        List<EnumVal> runningEnumVals =
            runningEnumValsMap.containsKey(attrName)
                ? runningEnumValsMap.get(attrName)
                : new ArrayList<>();
        runningEnumVals.add(new EnumVal(new ValueDisplay(val, display.get()), count.getAsLong()));
        runningEnumValsMap.put(attrName, runningEnumVals);
      }
    }
    runningEnumValsMap.entrySet().stream()
        .forEach(entry -> displayHints.put(entry.getKey(), new EnumVals(entry.getValue())));

    return displayHints;
  }

  public Attribute getAttribute(Entity entity, String attributeName) {
    Attribute attribute = entity.getAttribute(attributeName);
    if (attribute == null) {
      throw new NotFoundException(
          "Attribute not found: " + entity.getName() + ", " + attributeName);
    }
    return attribute;
  }

  public Hierarchy getHierarchy(Entity entity, String hierarchyName) {
    Hierarchy hierarchy = entity.getHierarchy(hierarchyName);
    if (hierarchy == null) {
      throw new NotFoundException("Hierarchy not found: " + hierarchyName);
    }
    return hierarchy;
  }

  public Relationship getRelationship(
      Collection<EntityGroup> entityGroups, Entity entity, Entity relatedEntity) {
    for (EntityGroup entityGroup : entityGroups) {
      Optional<Relationship> relationship = entityGroup.getRelationship(entity, relatedEntity);
      if (relationship.isPresent()) {
        return relationship.get();
      }
    }
    throw new NotFoundException(
        "Relationship not found for entities: "
            + entity.getName()
            + " -- "
            + relatedEntity.getName());
  }
}

package bio.terra.service.cohortbuilder;

import bio.terra.model.CountQuery;
import bio.terra.model.DataType;
import bio.terra.model.Instance;
import bio.terra.model.InstanceCountList;
import bio.terra.model.InstanceHierarchyFields;
import bio.terra.model.InstanceList;
import bio.terra.model.InstanceRelationshipFields;
import bio.terra.model.Literal;
import bio.terra.model.LiteralValueUnion;
import bio.terra.model.Query;
import bio.terra.model.ValueDisplay;
import bio.terra.tanagra.service.FromApiConversionService;
import bio.terra.tanagra.service.QuerysService;
import bio.terra.tanagra.service.UnderlaysService;
import bio.terra.tanagra.service.instances.EntityInstanceCount;
import bio.terra.tanagra.service.instances.filter.EntityFilter;
import bio.terra.tanagra.service.utils.ToApiConversionUtils;
import bio.terra.tanagra.underlay.Attribute;
import bio.terra.tanagra.underlay.Underlay;
import java.util.ArrayList;
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
        .sql(queryRequest.getSql());
  }

  public InstanceList queryInstances(String entityName, Query query) {
    List<InstanceRelationshipFields> relationshipFields =
        List.of(
            new InstanceRelationshipFields()
                .relatedEntity("person")
                .hierarchy("standard")
                .count(1970071),
            new InstanceRelationshipFields().relatedEntity("person").count(0));

    List<InstanceHierarchyFields> hierarchyFields =
        List.of(new InstanceHierarchyFields().hierarchy("standard").path("").numChildren(29));

    Map<String, ValueDisplay> attributes =
        Map.of(
            "standard_concept",
            new ValueDisplay()
                .display("Standard")
                .value(
                    new Literal()
                        .dataType(DataType.STRING)
                        .valueUnion(new LiteralValueUnion().stringVal("S"))),
            "vocabulary",
            new ValueDisplay()
                .display("Systematic Nomenclature of Medicine - Clinical Terms (IHTSDO)")
                .value(
                    new Literal()
                        .dataType(DataType.STRING)
                        .valueUnion(new LiteralValueUnion().stringVal("SNOMED"))),
            "name",
            new ValueDisplay()
                .value(
                    new Literal()
                        .dataType(DataType.STRING)
                        .valueUnion(new LiteralValueUnion().stringVal("Clinical finding"))),
            "concept_code",
            new ValueDisplay()
                .value(
                    new Literal()
                        .dataType(DataType.STRING)
                        .valueUnion(new LiteralValueUnion().stringVal("404684003"))),
            "id",
            new ValueDisplay()
                .value(
                    new Literal()
                        .dataType(DataType.INT64)
                        .valueUnion(new LiteralValueUnion().int64Val(441840L))));

    List<Instance> instances =
        List.of(
            new Instance()
                .relationshipFields(relationshipFields)
                .hierarchyFields(hierarchyFields)
                .attributes(attributes));
    return new InstanceList().sql("SQL Goes here").instances(instances);
  }
}

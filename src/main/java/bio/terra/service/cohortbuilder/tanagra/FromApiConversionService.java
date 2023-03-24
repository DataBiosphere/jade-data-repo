package bio.terra.service.cohortbuilder.tanagra;

import bio.terra.model.AttributeFilter;
import bio.terra.model.BooleanLogicFilter;
import bio.terra.model.Filter;
import bio.terra.model.HierarchyFilter;
import bio.terra.model.Literal;
import bio.terra.model.RelationshipFilter;
import bio.terra.model.TextFilter;
import bio.terra.service.cohortbuilder.tanagra.instances.filter.BooleanAndOrFilter;
import bio.terra.service.cohortbuilder.tanagra.instances.filter.BooleanNotFilter;
import bio.terra.service.cohortbuilder.tanagra.instances.filter.EntityFilter;
import bio.terra.service.cohortbuilder.tanagra.instances.filter.HierarchyAncestorFilter;
import bio.terra.service.cohortbuilder.tanagra.instances.filter.HierarchyMemberFilter;
import bio.terra.service.cohortbuilder.tanagra.instances.filter.HierarchyParentFilter;
import bio.terra.service.cohortbuilder.tanagra.instances.filter.HierarchyRootFilter;
import bio.terra.service.cohortbuilder.tanagra.service.QuerysService;
import bio.terra.service.cohortbuilder.tanagra.service.UnderlaysService;
import bio.terra.tanagra.exception.InvalidQueryException;
import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.query.filtervariable.BinaryFilterVariable;
import bio.terra.tanagra.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.tanagra.query.filtervariable.FunctionFilterVariable;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.EntityGroup;
import bio.terra.tanagra.underlay.Hierarchy;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public final class FromApiConversionService {
  private final UnderlaysService underlaysService;
  private final QuerysService querysService;

  @Autowired
  public FromApiConversionService(UnderlaysService underlaysService, QuerysService querysService) {
    this.underlaysService = underlaysService;
    this.querysService = querysService;
  }

  public EntityFilter fromApiObject(Filter apiFilter, Entity entity) {
    switch (apiFilter.getFilterType()) {
      case ATTRIBUTE:
        AttributeFilter apiAttributeFilter = apiFilter.getFilterUnion().getAttributeFilter();
        return new bio.terra.service.cohortbuilder.tanagra.instances.filter.AttributeFilter(
            querysService.getAttribute(entity, apiAttributeFilter.getAttribute()),
            FromApiConversionService.fromApiObject(apiAttributeFilter.getOperator()),
            FromApiConversionService.fromApiObject(apiAttributeFilter.getValue()));
      case TEXT:
        TextFilter apiTextFilter = apiFilter.getFilterUnion().getTextFilter();
        bio.terra.service.cohortbuilder.tanagra.instances.filter.TextFilter.Builder
            textFilterBuilder =
                new bio.terra.service.cohortbuilder.tanagra.instances.filter.TextFilter.Builder()
                    .textSearch(entity.getTextSearch())
                    .functionTemplate(
                        FromApiConversionService.fromApiObject(apiTextFilter.getMatchType()))
                    .text(apiTextFilter.getText());
        if (apiTextFilter.getAttribute() != null) {
          textFilterBuilder.attribute(
              querysService.getAttribute(entity, apiTextFilter.getAttribute()));
        }
        return textFilterBuilder.build();
      case HIERARCHY:
        HierarchyFilter apiHierarchyFilter = apiFilter.getFilterUnion().getHierarchyFilter();
        Hierarchy hierarchy = querysService.getHierarchy(entity, apiHierarchyFilter.getHierarchy());
        switch (apiHierarchyFilter.getOperator()) {
          case IS_ROOT:
            return new HierarchyRootFilter(hierarchy);
          case IS_MEMBER:
            return new HierarchyMemberFilter(hierarchy);
          case CHILD_OF:
            return new HierarchyParentFilter(
                hierarchy, FromApiConversionService.fromApiObject(apiHierarchyFilter.getValue()));
          case DESCENDANT_OF_INCLUSIVE:
            return new HierarchyAncestorFilter(
                hierarchy, FromApiConversionService.fromApiObject(apiHierarchyFilter.getValue()));
          default:
            throw new SystemException(
                "Unknown API hierarchy filter operator: " + apiHierarchyFilter.getOperator());
        }
      case RELATIONSHIP:
        RelationshipFilter apiRelationshipFilter =
            apiFilter.getFilterUnion().getRelationshipFilter();
        Entity relatedEntity = underlaysService.getEntity(apiRelationshipFilter.getEntity());
        // TODO: Allow building queries against the source data mapping also.
        EntityFilter subFilter = fromApiObject(apiRelationshipFilter.getSubfilter(), relatedEntity);

        Collection<EntityGroup> entityGroups =
            underlaysService.getUnderlay("cms_synpuf").getEntityGroups().values();
        return new bio.terra.service.cohortbuilder.tanagra.instances.filter.RelationshipFilter(
            entity, querysService.getRelationship(entityGroups, entity, relatedEntity), subFilter);
      case BOOLEAN_LOGIC:
        BooleanLogicFilter apiBooleanLogicFilter =
            apiFilter.getFilterUnion().getBooleanLogicFilter();
        List<EntityFilter> subFilters =
            apiBooleanLogicFilter.getSubfilters().stream()
                .map(apiSubFilter -> fromApiObject(apiSubFilter, entity))
                .collect(Collectors.toList());
        switch (apiBooleanLogicFilter.getOperator()) {
          case NOT -> {
            if (subFilters.size() != 1) {
              throw new InvalidQueryException(
                  "Boolean logic operator NOT can only have one sub-filter specified");
            }
            return new BooleanNotFilter(subFilters.get(0));
          }
          case OR, AND -> {
            if (subFilters.size() < 2) { // NOPMD - Allow using a literal in this conditional.
              throw new InvalidQueryException(
                  "Boolean logic operators OR, AND must have more than one sub-filter specified");
            }
            return new BooleanAndOrFilter(
                BooleanAndOrFilterVariable.LogicalOperator.valueOf(
                    apiBooleanLogicFilter.getOperator().name()),
                subFilters);
          }
          default -> throw new SystemException(
              "Unknown boolean logic operator: " + apiBooleanLogicFilter.getOperator());
        }
      default:
        throw new SystemException("Unknown API filter type: " + apiFilter.getFilterType());
    }
  }

  public static bio.terra.tanagra.query.Literal fromApiObject(Literal apiLiteral) {
    switch (apiLiteral.getDataType()) {
      case INT64:
        return new bio.terra.tanagra.query.Literal(apiLiteral.getValueUnion().getInt64Val());
      case STRING:
        return new bio.terra.tanagra.query.Literal(apiLiteral.getValueUnion().getStringVal());
      case BOOLEAN:
        return new bio.terra.tanagra.query.Literal(apiLiteral.getValueUnion().isBoolVal());
      case DATE:
        return bio.terra.tanagra.query.Literal.forDate(apiLiteral.getValueUnion().getDateVal());
      default:
        throw new SystemException("Unknown API data type: " + apiLiteral.getDataType());
    }
  }

  public static BinaryFilterVariable.BinaryOperator fromApiObject(
      AttributeFilter.OperatorEnum apiOperator) {
    return BinaryFilterVariable.BinaryOperator.valueOf(apiOperator.name());
  }

  public static FunctionFilterVariable.FunctionTemplate fromApiObject(
      TextFilter.MatchTypeEnum apiMatchType) {
    switch (apiMatchType) {
      case EXACT_MATCH:
        return FunctionFilterVariable.FunctionTemplate.TEXT_EXACT_MATCH;
      case FUZZY_MATCH:
        return FunctionFilterVariable.FunctionTemplate.TEXT_FUZZY_MATCH;
      default:
        throw new SystemException("Unknown API text match type: " + apiMatchType.name());
    }
  }
}

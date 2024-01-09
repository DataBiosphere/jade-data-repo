package bio.terra.tanagra.service.utils;

import autovalue.shaded.com.google.common.base.Preconditions;
import bio.terra.model.Filter;
import bio.terra.model.RelationshipFilter;

public final class ValidationUtils {
  private ValidationUtils() {}

  public static void validateApiFilter(Filter filter) {
    // If one RelationshipFilterV2 group_by field is set, all group_by fields must be set.
    if (filter != null && filter.getFilterType() == Filter.FilterTypeEnum.RELATIONSHIP) {
      RelationshipFilter relationshipFilter = filter.getFilterUnion().getRelationshipFilter();
      Preconditions.checkState(
          (relationshipFilter.getGroupByCountAttribute() == null
                  && relationshipFilter.getGroupByCountOperator() == null
                  && relationshipFilter.getGroupByCountValue() == null)
              || (relationshipFilter.getGroupByCountAttribute() != null
                  && relationshipFilter.getGroupByCountOperator() != null
                  && relationshipFilter.getGroupByCountValue() != null),
          "If one RelationshipFilterV2 group_by field is set, all group_by fields must be set");
    }
  }
}

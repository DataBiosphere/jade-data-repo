package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderSettings;
import org.springframework.stereotype.Component;

@Component
public class QueryBuilderFactory {

  public static final String HAS_CHILDREN = "has_children";
  public static final String COUNT = "count";
  public static final String PARENT_ID = "parent_id";

  public CriteriaQueryBuilder criteriaQueryBuilder(SnapshotBuilderSettings snapshotBuilderSettings) {
    return new CriteriaQueryBuilder(snapshotBuilderSettings);
  }

  public HierarchyQueryBuilder hierarchyQueryBuilder() {
    return new HierarchyQueryBuilder();
  }

  public ConceptChildrenQueryBuilder conceptChildrenQueryBuilder() {
    return new ConceptChildrenQueryBuilder();
  }

  public SearchConceptsQueryBuilder searchConceptsQueryBuilder() {
    return new SearchConceptsQueryBuilder();
  }
}

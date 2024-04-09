package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderSettings;
import org.springframework.stereotype.Component;

@Component
public class QueryBuilderFactory {
  public CriteriaQueryBuilder criteriaQueryBuilder(
      String rootTableName, SnapshotBuilderSettings snapshotBuilderSettings) {
    return new CriteriaQueryBuilder(rootTableName, snapshotBuilderSettings);
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

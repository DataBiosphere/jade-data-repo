package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import org.springframework.stereotype.Component;

@Component
public class QueryBuilderFactory {
  public CriteriaQueryBuilder criteriaQueryBuilder(
      String rootTableName,
      TableNameGenerator tableNameGenerator,
      SnapshotBuilderSettings snapshotBuilderSettings) {
    return new CriteriaQueryBuilder(rootTableName, tableNameGenerator, snapshotBuilderSettings);
  }

  public HierarchyQueryBuilder hierarchyQueryBuilder(TableNameGenerator tableNameGenerator) {
    return new HierarchyQueryBuilder(tableNameGenerator);
  }

  public ConceptChildrenQueryBuilder conceptChildrenQueryBuilder(
      TableNameGenerator tableNameGenerator) {
    return new ConceptChildrenQueryBuilder(tableNameGenerator);
  }
}

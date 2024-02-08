package bio.terra.service.snapshotbuilder.utils;

import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.service.snapshotbuilder.query.TableNameGenerator;
import org.springframework.stereotype.Component;

@Component
public class CriteriaQueryBuilderFactory {
  public CriteriaQueryBuilder createCriteriaQueryBuilder(
      String rootTableName,
      TableNameGenerator tableNameGenerator,
      SnapshotBuilderSettings snapshotBuilderSettings) {
    return new CriteriaQueryBuilder(rootTableName, tableNameGenerator, snapshotBuilderSettings);
  }
}

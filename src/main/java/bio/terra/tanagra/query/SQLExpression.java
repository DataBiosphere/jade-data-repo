package bio.terra.tanagra.query;

import bio.terra.model.CloudPlatform;

public interface SQLExpression {
  String renderSQL(CloudPlatform platform);

  default String renderSQL() {
    return renderSQL(CloudPlatform.GCP);
  }
}

package bio.terra.service.snapshotbuilder.query;

import bio.terra.common.CloudPlatformWrapper;

public interface SqlExpression {
  String renderSQL(CloudPlatformWrapper platform);

  // maybe add static no-op sqlexpression to avoid null checks
}

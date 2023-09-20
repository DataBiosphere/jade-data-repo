package bio.terra.service.filedata;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import java.util.List;
import java.util.stream.Stream;

public interface CloudFileReader {

  Stream<String> getBlobsLinesStream(
      String blobUrl, String cloudEncapsulationId, AuthenticatedUserRequest userRequest);

  void validateUserCanRead(
      List<String> sourcePaths,
      String cloudEncapsulationId,
      AuthenticatedUserRequest user,
      Dataset dataset);
}

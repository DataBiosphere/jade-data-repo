package bio.terra.service.filedata;

import bio.terra.service.iam.AuthenticatedUserRequest;
import java.util.stream.Stream;

public interface CloudFileReader {

  Stream<String> getBlobsLinesStream(
      String blobUrl, String cloudEncapsulationId, AuthenticatedUserRequest userRequest);
}

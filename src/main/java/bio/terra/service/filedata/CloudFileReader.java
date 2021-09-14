package bio.terra.service.filedata;

import java.util.stream.Stream;

public interface CloudFileReader {

  Stream<String> getBlobsLinesStream(String blobUrl, String cloudEncapsulationId);

  void writeStreamToCloudFile(String path, Stream<String> contentsToWrite, String cloudEncapsulationId);

}

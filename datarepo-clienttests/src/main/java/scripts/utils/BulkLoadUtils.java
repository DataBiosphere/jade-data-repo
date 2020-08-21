package scripts.utils;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.model.*;
import common.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BulkLoadUtils {
  private static final Logger logger = LoggerFactory.getLogger(BulkLoadUtils.class);

  // This should run about 5 minutes on 2 DRmanager instances. The speed of loads is when
  // they are not local is about 2.5GB/minutes. With a fixed size of 1GB, each instance should do
  // 2.5 files per minute, so two instances should do 5 files per minute. To run 5 minutes we should
  // run 25 files.
  public static BulkLoadArrayRequestModel buildBulkLoadFileRequest(
      int filesToLoad, String billingProfileId, String datasetId) {
    String loadTag = FileUtils.randomizeName("longtest");

    BulkLoadArrayRequestModel arrayLoad =
        new BulkLoadArrayRequestModel()
            .profileId(billingProfileId)
            .loadTag(loadTag)
            .maxFailedFileLoads(filesToLoad); // do not stop if there is a failure.

    logger.debug("longFileLoadTest loading {} files into dataset id {}", filesToLoad, datasetId);

    // There are currently 26 source files, so if ingesting more files: continue to loop through the
    // source files,
    // but point to new target files file paths
    int numberOfSourceFiles = 26;
    for (int i = 0; i < filesToLoad; i++) {
      String fileBasePath = "/fileloadscaletest/file1GB-%02d.txt";
      String sourcePath =
          "gs://jade-testdata-uswestregion" + String.format(fileBasePath, i % numberOfSourceFiles);
      String targetPath = "/" + loadTag + String.format(fileBasePath, i);

      BulkLoadFileModel model = new BulkLoadFileModel().mimeType("application/binary");
      model.description("bulk load file " + i).sourcePath(sourcePath).targetPath(targetPath);
      arrayLoad.addLoadArrayItem(model);
    }

    return arrayLoad;
  }

  public static BulkLoadResultModel getAndDisplayResults(
      RepositoryApi repositoryApi, JobModel bulkLoadArrayJobResponse) throws Exception {
    bulkLoadArrayJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, bulkLoadArrayJobResponse);

    BulkLoadArrayResultModel result =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, bulkLoadArrayJobResponse, BulkLoadArrayResultModel.class);

    BulkLoadResultModel loadSummary = result.getLoadSummary();

    logger.debug("Total files    : {}", loadSummary.getTotalFiles());
    logger.debug("Succeeded files: {}", loadSummary.getSucceededFiles());
    logger.debug("Failed files   : {}", loadSummary.getFailedFiles());
    logger.debug("Not Tried files: {}", loadSummary.getNotTriedFiles());

    return loadSummary;
  }
}

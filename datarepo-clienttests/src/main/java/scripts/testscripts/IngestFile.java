package scripts.testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.BulkLoadFileResultModel;
import bio.terra.datarepo.model.JobModel;
import common.utils.FileUtils;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.TestUserSpecification;
import scripts.testscripts.baseclasses.SimpleDataset;
import scripts.utils.DataRepoUtils;

public class IngestFile extends SimpleDataset {
  private static final Logger logger = LoggerFactory.getLogger(IngestFile.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public IngestFile() {
    super();
  }

  private URI sourceFileURI;

  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() == 0) {
      throw new IllegalArgumentException(
          "Must provide a URI for the source file in the parameters list");
    } else
      try {
        sourceFileURI = new URI(parameters.get(0));
      } catch (URISyntaxException synEx) {
        throw new RuntimeException("Error parsing source file URI: " + parameters.get(0), synEx);
      }
    logger.debug("Source file URI: {}", sourceFileURI);
  }

  public void userJourney(TestUserSpecification testUser) throws Exception {
    ApiClient apiClient = DataRepoUtils.getClientForTestUser(testUser, server);
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    String targetPath = "/testrunner/IngestFile/" + FileUtils.randomizeName("") + ".txt";

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .sourcePath(sourceFileURI.toString())
            .description("IngestFile")
            .mimeType("text/plain")
            .targetPath(targetPath);

    String loadTag = FileUtils.randomizeName("lookupTest");
    BulkLoadArrayRequestModel fileLoadModelArray =
        new BulkLoadArrayRequestModel()
            .profileId(datasetSummaryModel.getDefaultProfileId())
            .loadTag(loadTag)
            .maxFailedFileLoads(0);
    fileLoadModelArray.addLoadArrayItem(fileLoadModel);

    JobModel ingestFileJobResponse =
        repositoryApi.bulkFileLoadArray(datasetSummaryModel.getId(), fileLoadModelArray);

    ingestFileJobResponse =
        DataRepoUtils.waitForJobToFinish(repositoryApi, ingestFileJobResponse, testUser);

    BulkLoadArrayResultModel bulkLoadArrayResultModel =
        DataRepoUtils.expectJobSuccess(
            repositoryApi, ingestFileJobResponse, BulkLoadArrayResultModel.class);
    BulkLoadFileResultModel fileInfo = bulkLoadArrayResultModel.getLoadFileResults().get(0);

    logger.debug(
        "Successfully ingested file: path = {}, id = {}",
        fileInfo.getSourcePath(),
        fileInfo.getFileId());
  }
}

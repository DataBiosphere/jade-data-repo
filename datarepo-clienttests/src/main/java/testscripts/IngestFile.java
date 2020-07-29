package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadArrayResultModel;
import bio.terra.datarepo.model.BulkLoadFileModel;
import bio.terra.datarepo.model.BulkLoadFileResultModel;
import bio.terra.datarepo.model.JobModel;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testscripts.baseclasses.SimpleDataset;
import utils.DataRepoUtils;
import utils.FileUtils;

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

  public void userJourney(ApiClient apiClient) throws Exception {
    RepositoryApi repositoryApi = new RepositoryApi(apiClient);

    String targetPath = "/testrunner/IngestFile/" + FileUtils.randomizeName("") + ".txt";

    BulkLoadFileModel fileLoadModel =
        new BulkLoadFileModel()
            .sourcePath(sourceFileURI.toString())
            .description("IngestFile")
            .mimeType("text/plain")
            .targetPath(targetPath);
    List<BulkLoadFileModel> bulkLoadFileModelList = new ArrayList<>();
    bulkLoadFileModelList.add(fileLoadModel);
    BulkLoadArrayRequestModel fileLoadModelArray =
        new BulkLoadArrayRequestModel()
            .profileId(datasetSummaryModel.getDefaultProfileId())
            .loadArray(bulkLoadFileModelList);
    JobModel ingestFileJobResponse =
        repositoryApi.bulkFileLoadArray(datasetSummaryModel.getId(), fileLoadModelArray);

    ingestFileJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, ingestFileJobResponse);

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

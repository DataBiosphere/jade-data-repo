package testscripts;

import bio.terra.datarepo.api.RepositoryApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DatasetRequestModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.ErrorModel;
import bio.terra.datarepo.model.JobModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import utils.DataRepoUtils;
import utils.FileUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RetrieveDataset extends runner.TestScript {

    /**
     * Public constructor so that this class can be instantiated via reflection.
     */
    public RetrieveDataset() {
        super();
    }

    private String datasetCreator;
    private DatasetSummaryModel datasetSummaryModel;

    public void setup(Map<String, ApiClient> apiClients) throws Exception {
        // pick the first user to be the dataset creator
        List<String> apiClientList = new ArrayList<>(apiClients.keySet());
        datasetCreator = apiClientList.get(0);

        // get the ApiClient for the dataset creator
        ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
        RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

        // use Jackson to map the stream contents to a DatasetRequestModel object
        ObjectMapper objectMapper = new ObjectMapper();
        InputStream datasetRequestFile = FileUtils.getJSONFileHandle("apipayloads/dataset-simple.json");
        DatasetRequestModel createDatasetRequest = objectMapper.readValue(datasetRequestFile, DatasetRequestModel.class);
        createDatasetRequest.setName(FileUtils.randomizeName(createDatasetRequest.getName()));

        // make the create request and wait for the job to finish
        JobModel createDatasetJobResponse = repositoryApi.createDataset(createDatasetRequest);
        createDatasetJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, createDatasetJobResponse);
        if (createDatasetJobResponse.getJobStatus().equals(JobModel.JobStatusEnum.FAILED)) {
            ErrorModel errorModel = DataRepoUtils.getJobResult(repositoryApi, createDatasetJobResponse, ErrorModel.class);
            throw new RuntimeException("Failed to create dataset: " + createDatasetRequest.getName() + ". errorModel=" + errorModel);
        }

        // save a reference to the dataset summary model so we can delete it in cleanup()
        datasetSummaryModel = DataRepoUtils.getJobResult(repositoryApi, createDatasetJobResponse, DatasetSummaryModel.class);

        System.out.println("successfully created dataset: " + datasetSummaryModel.getName());
    }

    public void userJourney(ApiClient apiClient) throws Exception {
        RepositoryApi repositoryApi = new RepositoryApi(apiClient);
        DatasetModel datasetModel = repositoryApi.retrieveDataset(datasetSummaryModel.getId());

        System.out.println("successfully retrieved dataset: " + datasetModel.getName() + ", data project: " + datasetModel.getDataProject());
    }

    public void cleanup(Map<String, ApiClient> apiClients) throws Exception {
        // get the ApiClient for the dataset creator
        ApiClient datasetCreatorClient = apiClients.get(datasetCreator);
        RepositoryApi repositoryApi = new RepositoryApi(datasetCreatorClient);

        // make the delete request and wait for the job to finish
        JobModel deleteDatasetJobResponse = repositoryApi.deleteDataset(datasetSummaryModel.getId());
        deleteDatasetJobResponse = DataRepoUtils.waitForJobToFinish(repositoryApi, deleteDatasetJobResponse);
        if (deleteDatasetJobResponse.getJobStatus().equals(JobModel.JobStatusEnum.FAILED)) {
            ErrorModel errorModel = DataRepoUtils.getJobResult(repositoryApi, deleteDatasetJobResponse, ErrorModel.class);
            throw new RuntimeException("Failed to delete dataset: " + datasetSummaryModel.getName() + ". errorModel=" + errorModel);
        }

        System.out.println("successfully deleted dataset: " + datasetSummaryModel.getName());
    }
}

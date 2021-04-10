package bio.terra.datarepo.api;

import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.client.Configuration;
import bio.terra.datarepo.model.AssetModel;
import bio.terra.datarepo.model.BulkLoadArrayRequestModel;
import bio.terra.datarepo.model.BulkLoadRequestModel;
import bio.terra.datarepo.model.ConfigEnableModel;
import bio.terra.datarepo.model.ConfigGroupModel;
import bio.terra.datarepo.model.ConfigListModel;
import bio.terra.datarepo.model.ConfigModel;
import bio.terra.datarepo.model.DataDeletionRequest;
import bio.terra.datarepo.model.DatasetModel;
import bio.terra.datarepo.model.DatasetRequestModel;
import bio.terra.datarepo.model.EnumerateDatasetModel;
import bio.terra.datarepo.model.EnumerateSnapshotModel;
import bio.terra.datarepo.model.FileLoadModel;
import bio.terra.datarepo.model.FileModel;
import bio.terra.datarepo.model.IngestRequestModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.PolicyResponse;
import bio.terra.datarepo.model.RepositoryConfigurationModel;
import bio.terra.datarepo.model.RepositoryStatusModel;
import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.datarepo.model.SnapshotRequestModel;
import bio.terra.datarepo.model.UpgradeModel;
import bio.terra.datarepo.model.UserStatusInfo;

import java.util.List;

@Deprecated(since = "2021/04/10", forRemoval = true)
public class RepositoryApi {

    private ApiClient apiClient;
    private final ConfigsApi configsApi;
    private final DatasetsApi datasetsApi;
    private final JobsApi jobsApi;
    private final RegisterApi registerApi;
    private final SnapshotsApi snapshotsApi;
    private final UnauthenticatedApi unauthenticatedApi;
    private final UpgradeApi upgradeApi;


    public RepositoryApi() {
        this(Configuration.getDefaultApiClient());
    }

    public RepositoryApi(ApiClient apiClient) {
        this.apiClient = apiClient;
        this.configsApi = new ConfigsApi(apiClient);
        this.datasetsApi = new DatasetsApi(apiClient);
        this.jobsApi = new JobsApi(apiClient);
        this.registerApi = new RegisterApi(apiClient);
        this.snapshotsApi = new SnapshotsApi(apiClient);
        this.unauthenticatedApi = new UnauthenticatedApi(apiClient);
        this.upgradeApi = new UpgradeApi(apiClient);
    }

    public ApiClient RepositoryApi() {
        return apiClient;
    }

    public void RepositoryApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }


    // Configs
    public ConfigModel getConfig(String name) throws ApiException {
        return configsApi.getConfig(name);
    }

    public ConfigListModel getConfigList() throws ApiException {
        return configsApi.getConfigList();
    }

    public void resetConfig() throws ApiException {
        configsApi.resetConfig();
    }

    public ConfigListModel setConfigList(ConfigGroupModel body) throws ApiException {
        return configsApi.setConfigList(body);
    }

    public void setFault(String name, ConfigEnableModel body) throws ApiException {
        configsApi.setFault(name, body);
    }

    // Datasets
    public JobModel addDatasetAssetSpecifications(String id, AssetModel body) throws ApiException {
        return datasetsApi.addDatasetAssetSpecifications(id, body);
    }

    public PolicyResponse addDatasetPolicyMember(String id, String policyName, PolicyMemberRequest body) throws ApiException {
        return datasetsApi.addDatasetPolicyMember(id, policyName, body);
    }

    public JobModel applyDatasetDataDeletion(String id, DataDeletionRequest body) throws ApiException {
        return datasetsApi.applyDatasetDataDeletion(id, body);
    }

    public JobModel bulkFileLoad(String id, BulkLoadRequestModel body) throws ApiException {
        return datasetsApi.bulkFileLoad(id, body);
    }

    public JobModel bulkFileLoadArray(String id, BulkLoadArrayRequestModel body) throws ApiException {
        return datasetsApi.bulkFileLoadArray(id, body);
    }

    public JobModel bulkFileResultsDelete(String id, String loadtag, String jobId) throws ApiException {
        return datasetsApi.bulkFileResultsDelete(id, loadtag, jobId);
    }

    public JobModel bulkFileResultsGet(String id, String loadtag, String jobId, Integer offset, Integer limit) throws ApiException {
        return datasetsApi.bulkFileResultsGet(id, loadtag, jobId, offset, limit);
    }

    public JobModel createDataset(DatasetRequestModel body) throws ApiException {
        return datasetsApi.createDataset(body);
    }

    public JobModel deleteDataset(String id) throws ApiException {
        return datasetsApi.deleteDataset(id);
    }

    public PolicyResponse deleteDatasetPolicyMember(String id, String policyName, String memberEmail) throws ApiException {
        return datasetsApi.deleteDatasetPolicyMember(id, policyName, memberEmail);
    }

    public JobModel deleteFile(String id, String fileid) throws ApiException {
        return datasetsApi.deleteFile(id, fileid);
    }

    public EnumerateDatasetModel enumerateDatasets(Integer offset, Integer limit, String sort, String direction, String filter) throws ApiException {
        return datasetsApi.enumerateDatasets(offset, limit, sort, direction, filter);
    }

    public JobModel ingestDataset(String id, IngestRequestModel body) throws ApiException {
        return datasetsApi.ingestDataset(id, body);
    }

    public JobModel ingestFile(String id, FileLoadModel body) throws ApiException {
        return datasetsApi.ingestFile(id, body);
    }

    public FileModel lookupFileById(String id, String fileid, Integer depth) throws ApiException {
        return datasetsApi.lookupFileById(id, fileid, depth);
    }

    public FileModel lookupFileByPath(String id, String path, Integer depth) throws ApiException {
        return datasetsApi.lookupFileByPath(id, path, depth);
    }

    public JobModel removeDatasetAssetSpecifications(String id, String assetid) throws ApiException {
        return datasetsApi.removeDatasetAssetSpecifications(id, assetid);
    }

    public DatasetModel retrieveDataset(String id) throws ApiException {
        return datasetsApi.retrieveDataset(id);
    }

    public PolicyResponse retrieveDatasetPolicies(String id) throws ApiException {
        return datasetsApi.retrieveDatasetPolicies(id);
    }

    // Jobs
    public void deleteJob(String id) throws ApiException {
        jobsApi.deleteJob(id);
    }

    public List<JobModel> enumerateJobs(Integer offset, Integer limit) throws ApiException {
        return jobsApi.enumerateJobs(offset, limit);
    }

    public JobModel retrieveJob(String id) throws ApiException {
        return jobsApi.retrieveJob(id);
    }

    public Object retrieveJobResult(String id) throws ApiException {
        return jobsApi.retrieveJobResult(id);
    }

    // Register
    public UserStatusInfo user() throws ApiException {
        return registerApi.user();
    }

    // Snapshots
    public PolicyResponse addSnapshotPolicyMember(String id, String policyName, PolicyMemberRequest body) throws ApiException {
        return snapshotsApi.addSnapshotPolicyMember(id, policyName, body);
    }

    public JobModel createSnapshot(SnapshotRequestModel body) throws ApiException {
        return snapshotsApi.createSnapshot(body);
    }

    public JobModel deleteSnapshot(String id) throws ApiException {
        return snapshotsApi.deleteSnapshot(id);
    }

    public PolicyResponse deleteSnapshotPolicyMember(String id, String policyName, String memberEmail) throws ApiException {
        return snapshotsApi.deleteSnapshotPolicyMember(id, policyName, memberEmail);
    }

    public EnumerateSnapshotModel enumerateSnapshots(Integer offset, Integer limit, String sort, String direction, String filter, List<String> datasetIds) throws ApiException {
        return snapshotsApi.enumerateSnapshots(offset, limit, sort, direction, filter, datasetIds);
    }

    public FileModel lookupSnapshotFileById(String id, String fileid, Integer depth) throws ApiException {
        return snapshotsApi.lookupSnapshotFileById(id, fileid, depth);
    }

    public FileModel lookupSnapshotFileByPath(String id, String path, Integer depth) throws ApiException {
        return snapshotsApi.lookupSnapshotFileByPath(id, path, depth);
    }

    public SnapshotModel retrieveSnapshot(String id) throws ApiException {
        return snapshotsApi.retrieveSnapshot(id);
    }

    public PolicyResponse retrieveSnapshotPolicies(String id) throws ApiException {
        return snapshotsApi.retrieveSnapshotPolicies(id);
    }

    // Unauthenticated
    public RepositoryConfigurationModel retrieveRepositoryConfig() throws ApiException {
        return unauthenticatedApi.retrieveRepositoryConfig();
    }

    public RepositoryStatusModel serviceStatus() throws ApiException {
        return unauthenticatedApi.serviceStatus();
    }

    public void shutdownRequest() throws ApiException {
        unauthenticatedApi.shutdownRequest();
    }

    //Upgrade
    public JobModel upgrade(UpgradeModel body) throws ApiException {
        return upgradeApi.upgrade(body);
    }
}

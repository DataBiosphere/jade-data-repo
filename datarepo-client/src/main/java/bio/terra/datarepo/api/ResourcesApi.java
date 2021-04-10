package bio.terra.datarepo.api;

import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import bio.terra.datarepo.client.Configuration;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.model.BillingProfileRequestModel;
import bio.terra.datarepo.model.BillingProfileUpdateModel;
import bio.terra.datarepo.model.EnumerateBillingProfileModel;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.PolicyMemberRequest;
import bio.terra.datarepo.model.PolicyResponse;

@Deprecated(since = "2021/04/10", forRemoval = true)
public class ResourcesApi extends ProfilesApi {

    private ApiClient apiClient;
    private final ProfilesApi profilesApi;


    public ResourcesApi() {
        this(Configuration.getDefaultApiClient());
    }

    public ResourcesApi(ApiClient apiClient) {
        this.apiClient = apiClient;
        this.profilesApi = new ProfilesApi(apiClient);
    }

    public ApiClient ResourcesApi() {
        return apiClient;
    }

    public void ResourcesApi(ApiClient apiClient) {
        this.apiClient = apiClient;
    }


    public PolicyResponse addProfilePolicyMember(PolicyMemberRequest body, String id, String policyName) throws ApiException {
        return profilesApi.addProfilePolicyMember(body, id, policyName);
    }

    public JobModel createProfile(BillingProfileRequestModel body) throws ApiException {
        return profilesApi.createProfile(body);
    }

    public JobModel deleteProfile(String id) throws ApiException {
        return profilesApi.deleteProfile(id);
    }

    public PolicyResponse deleteProfilePolicyMember(String id, String policyName, String memberEmail) throws ApiException {
        return profilesApi.deleteProfilePolicyMember(id, policyName, memberEmail);
    }

    public EnumerateBillingProfileModel enumerateProfiles(Integer offset, Integer limit) throws ApiException {
        return profilesApi.enumerateProfiles(offset, limit);
    }

    public BillingProfileModel retrieveProfile(String id) throws ApiException {
        return profilesApi.retrieveProfile(id);
    }

    public PolicyResponse retrieveProfilePolicies(String id) throws ApiException {
        return profilesApi.retrieveProfilePolicies(id);
    }

    public JobModel updateProfile(BillingProfileUpdateModel body) throws ApiException {
        return profilesApi.updateProfile(body);
    }
}

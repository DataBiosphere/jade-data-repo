package bio.terra.service.iam;

import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.exception.InternalServerErrorException;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.model.PolicyModel;
import bio.terra.model.UserStatusInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.Pair;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.UsersApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembership;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyResponseEntry;
import org.broadinstitute.dsde.workbench.client.sam.model.ResourceAndAccessPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class SamClientService {
    private final SamConfiguration samConfig;

    public SamClientService(SamConfiguration samConfig) {
        this.samConfig = samConfig;
    }

    public enum ResourceType {
        DATAREPO,
        DATASET,
        DATASNAPSHOT;

        @Override
        @JsonValue
        public String toString() {
            return StringUtils.lowerCase(name());
        }

        @JsonCreator
        public static ResourceType fromValue(String text) {
            for (ResourceType b : ResourceType.values()) {
                if (b.name().equals(StringUtils.upperCase(text))) {
                    return b;
                }
            }
            return null;
        }

    }

    public enum DataRepoRole {
        ADMIN,
        STEWARD,
        CUSTODIAN,
        INGESTER,
        READER,
        DISCOVERER;

        @Override
        @JsonValue
        public String toString() {
            return StringUtils.lowerCase(name());
        }

        @JsonCreator
        public static ResourceType fromValue(String text) {
            for (ResourceType b : ResourceType.values()) {
                if (String.valueOf(b.name()).equals(StringUtils.upperCase(text))) {
                    return b;
                }
            }
            return null;
        }

    }


    public enum DataRepoAction {
        // common
        CREATE,
        DELETE,
        SHARE_POLICY,
        READ_POLICY,
        READ_POLICIES,
        ALTER_POLICIES,
        // datarepo
        CREATE_DATASET,
        LIST_JOBS,
        DELETE_JOBS,
        // dataset
        EDIT_DATASET,
        READ_DATASET,
        INGEST_DATA,
        UPDATE_DATA,
        // snapshots
        CREATE_DATASNAPSHOT,
        EDIT_DATASNAPSHOT,
        READ_DATA,
        DISCOVER_DATA;

        @Override
        @JsonValue
        public String toString() {
            return StringUtils.lowerCase(name());
        }

        @JsonCreator
        public static ResourceType fromValue(String text) {
            for (ResourceType b : ResourceType.values()) {
                if (String.valueOf(b.name()).equals(StringUtils.upperCase(text))) {
                    return b;
                }
            }
            return null;
        }
    }

    private static Logger logger = LoggerFactory.getLogger(SamClientService.class);

    private ApiClient getApiClient(String accessToken) {
        ApiClient apiClient = new ApiClient();
        apiClient.setAccessToken(accessToken);
        apiClient.setUserAgent("OpenAPI-Generator/1.0.0 java");  // only logs an error in sam
        return apiClient.setBasePath(samConfig.getBasePath());
    }

    private ResourcesApi samResourcesApi(String accessToken) {
        return new ResourcesApi(getApiClient(accessToken));
    }

    private GoogleApi samGoogleApi(String accessToken) {
        return new GoogleApi(getApiClient(accessToken));
    }

    private UsersApi samUsersApi(String accessToken) {
        return new UsersApi(getApiClient(accessToken));
    }

    public boolean isAuthorized(
        AuthenticatedUserRequest userReq,
        SamClientService.ResourceType resourceType,
        String resourceId,
        SamClientService.DataRepoAction action) {
        boolean authorized = false;
        try {
            authorized = checkResourceAction(
                userReq,
                resourceType.toString(),
                resourceId,
                action.toString());
            logger.info("authorized is " + authorized);
        } catch (ApiException ex) {
            logger.warn("userReq token: {}", userReq.getToken());
            throw new InternalServerErrorException(ex);
        }
        return authorized;
    }

    public void verifyAuthorization(
        AuthenticatedUserRequest userReq,
        SamClientService.ResourceType resourceType,
        String resourceId,
        SamClientService.DataRepoAction action) {
        String userEmail = userReq.getEmail();
        logger.info("email: {}, action: {}", userEmail, action);
        if (!isAuthorized(userReq, resourceType, resourceId, action)) {
            throw new UnauthorizedException("User does not have required action: " + action);
        }
    }

    public List<ResourceAndAccessPolicy> listAuthorizedResources(
        AuthenticatedUserRequest userReq, ResourceType resourceType) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        return samResourceApi.listResourcesAndPolicies(resourceType.toString());
    }

    private boolean checkResourceAction(
        AuthenticatedUserRequest userReq,
        String samResourceType,
        String samResource,
        String action)
            throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        return samResourceApi.resourceAction(samResourceType, samResource, action);
    }

    public void deleteDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        samResourceApi.deleteResource(ResourceType.DATASET.toString(), datasetId.toString());
    }

    public void deleteSnapshotResource(AuthenticatedUserRequest userReq, UUID datsetId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        samResourceApi.deleteResource(ResourceType.DATASNAPSHOT.toString(), datsetId.toString());
    }

    public List<String> createDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId) throws ApiException {
        CreateResourceCorrectRequest req = new CreateResourceCorrectRequest();
        req.setResourceId(datasetId.toString());
        req.addPoliciesItem(
            DataRepoRole.STEWARD.toString(),
            createAccessPolicy(DataRepoRole.STEWARD.toString(),
                Collections.singletonList(samConfig.getStewardsGroupEmail())));
        req.addPoliciesItem(
            DataRepoRole.CUSTODIAN.toString(),
            createAccessPolicy(DataRepoRole.CUSTODIAN.toString(), Collections.singletonList(userReq.getEmail())));
        req.addPoliciesItem(
            DataRepoRole.INGESTER.toString(),
            new AccessPolicyMembership().roles(Collections.singletonList(DataRepoRole.INGESTER.toString())));

        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        logger.debug(req.toString());
        createResourceCorrectCall(samResourceApi.getApiClient(), ResourceType.DATASET.toString(), req);

        // we'll want all of these roles to have read access to the underlying data, so we sync and return the emails
        // for the policies that get created by SAM
        ArrayList<String> rolePolicies = new ArrayList<>();
        for (DataRepoRole role : Arrays.asList(DataRepoRole.STEWARD, DataRepoRole.CUSTODIAN, DataRepoRole.INGESTER)) {
            Map<String, List<Object>> results = samGoogleApi(userReq.getRequiredToken()).syncPolicy(
                ResourceType.DATASET.toString(),
                datasetId.toString(),
                role.toString());
            rolePolicies.add(getPolicyGroupEmailFromResponse(results));
        }
        return rolePolicies;
    }

    public String createSnapshotResource(
        AuthenticatedUserRequest userReq,
        UUID snapshotId,
        Optional<List<String>> readersList
    ) throws ApiException {
        CreateResourceCorrectRequest req = new CreateResourceCorrectRequest();


        req.setResourceId(snapshotId.toString());
        req.addPoliciesItem(
            DataRepoRole.STEWARD.toString(),
            createAccessPolicy(DataRepoRole.STEWARD.toString(),
                Collections.singletonList(samConfig.getStewardsGroupEmail())));
        req.addPoliciesItem(
            DataRepoRole.CUSTODIAN.toString(),
            createAccessPolicy(DataRepoRole.CUSTODIAN.toString(), Collections.singletonList(userReq.getEmail())));
        req.addPoliciesItem(
            DataRepoRole.READER.toString(),
            createAccessPolicy(DataRepoRole.READER.toString(), readersList.orElse(Collections.emptyList())));
        req.addPoliciesItem(
            DataRepoRole.DISCOVERER.toString(),
            new AccessPolicyMembership().roles(Collections.singletonList(DataRepoRole.DISCOVERER.toString())));

        // create the resource in sam
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        logger.debug(req.toString());
        createResourceCorrectCall(samResourceApi.getApiClient(), ResourceType.DATASNAPSHOT.toString(), req);

        // sync the readers policy
        // Map[WorkbenchEmail, Seq[SyncReportItem]]
        Map<String, List<Object>> results = samGoogleApi(userReq.getRequiredToken()).syncPolicy(
            ResourceType.DATASNAPSHOT.toString(),
            snapshotId.toString(),
            DataRepoRole.READER.toString());
        return getPolicyGroupEmailFromResponse(results);
    }

    public List<PolicyModel> retrievePolicies(
        AuthenticatedUserRequest userReq,
        ResourceType resourceType,
        UUID resourceId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        List<AccessPolicyResponseEntry> results =
            samResourceApi.listResourcePolicies(resourceType.toString(), resourceId.toString());
        return results.stream().map(entry -> new PolicyModel()
            .name(entry.getPolicyName())
            .members(entry.getPolicy().getMemberEmails()))
            .collect(Collectors.toList());

    }

    public PolicyModel addPolicyMember(
        AuthenticatedUserRequest userReq,
        ResourceType resourceType,
        UUID resourceId,
        String policyName,
        String userEmail) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        samResourceApi.addUserToPolicy(resourceType.toString(), resourceId.toString(), policyName, userEmail);

        AccessPolicyMembership result =
            samResourceApi.getPolicy(resourceType.toString(), resourceId.toString(), policyName);
        return new PolicyModel()
            .name(policyName)
            .members(result.getMemberEmails());

    }

    public PolicyModel deletePolicyMember(
        AuthenticatedUserRequest userReq,
        ResourceType resourceType,
        UUID resourceId,
        String policyName,
        String userEmail) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getRequiredToken());
        samResourceApi.removeUserFromPolicy(resourceType.toString(), resourceId.toString(), policyName, userEmail);

        AccessPolicyMembership result =
            samResourceApi.getPolicy(resourceType.toString(), resourceId.toString(), policyName);
        return new PolicyModel()
            .name(policyName)
            .members(result.getMemberEmails());

    }

    public UserStatusInfo getUserInfo(AuthenticatedUserRequest userReq) throws ApiException {
        UsersApi samUsersApi = samUsersApi(userReq.getRequiredToken());
        org.broadinstitute.dsde.workbench.client.sam.model.UserStatusInfo samInfo = samUsersApi.getUserStatusInfo();
        return new UserStatusInfo().userSubjectId(samInfo.getUserSubjectId())
            .userEmail(samInfo.getUserEmail())
            .enabled(samInfo.getEnabled());
    }

    public AccessPolicyMembership createAccessPolicy(String role, List<String> emails) {
        return new AccessPolicyMembership()
            .roles(Collections.singletonList(role))
            .memberEmails(emails);
    }

    // This is a work around for https://broadworkbench.atlassian.net/browse/AP-149
    // This is a copy of the ApiClient.createResourceCall but adds in the validation and
    // the actual execution of the call. And doesn't allow listener callbacks
    private void createResourceCorrectCall(
        ApiClient localVarApiClient,
        String resourceTypeName,
        CreateResourceCorrectRequest resourceCreate) throws ApiException {

        // verify the required parameter 'resourceTypeName' is set
        if (resourceTypeName == null) {
            throw new ApiException(
                "Missing the required parameter 'resourceTypeName' when calling createResource(Async)");
        }

        // verify the required parameter 'resourceCreate' is set
        if (resourceCreate == null) {
            throw new ApiException(
                "Missing the required parameter 'resourceCreate' when calling createResource(Async)");
        }

        // create path and map variables
        String localVarPath = "/api/resources/v1/" + localVarApiClient.escapeString(resourceTypeName);

        List<Pair> localVarQueryParams = new ArrayList<Pair>();
        List<Pair> localVarCollectionQueryParams = new ArrayList<Pair>();
        Map<String, String> localVarHeaderParams = new HashMap<String, String>();
        Map<String, Object> localVarFormParams = new HashMap<String, Object>();
        final String[] localVarAccepts = {
            "application/json"
        };
        final String localVarAccept = localVarApiClient.selectHeaderAccept(localVarAccepts);
        if (localVarAccept != null) {
            localVarHeaderParams.put("Accept", localVarAccept);
        }

        final String[] localVarContentTypes = {
        };
        final String localVarContentType = localVarApiClient.selectHeaderContentType(localVarContentTypes);
        localVarHeaderParams.put("Content-Type", localVarContentType);


        String[] localVarAuthNames = new String[] {"googleoauth"};
        okhttp3.Call localVarCall = localVarApiClient.buildCall(
            localVarPath,
            "POST",
            localVarQueryParams,
            localVarCollectionQueryParams,
            resourceCreate,
            localVarHeaderParams,
            localVarFormParams,
            localVarAuthNames,
            null);
        localVarApiClient.execute(localVarCall);
    }

    /**
     * Syncing a policy with SAM results in a Google group being created that is tied to that policy. The response is an
     * object with one key that is the policy group email and a value that is a list of objects.
     * @param syncPolicyResponse map with one key that is an email
     * @return the policy group email
     */
    private String getPolicyGroupEmailFromResponse(Map<String, List<Object>> syncPolicyResponse) {
        if (syncPolicyResponse.size() != 1) {
            throw new IllegalArgumentException("Expecting syncPolicyResponse to be an object with one key");
        }
        return syncPolicyResponse.keySet().iterator().next();
    }


}

package bio.terra.service;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.model.sam.CreateResourceCorrectRequest;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.Pair;
import org.broadinstitute.dsde.workbench.client.sam.api.GoogleApi;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.AccessPolicyMembership;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class SamClientService {

    public enum ResourceType {
        data_repository("data_repository"),
        study("study"),
        dataset("dataset");

        private String value;

        ResourceType(String value) {
            this.value = value;
        }

        @Override
        @JsonValue
        public String toString() {
            return String.valueOf(value);
        }

        @JsonCreator
        public static ResourceType fromValue(String text) {
            for (ResourceType b : ResourceType.values()) {
                if (String.valueOf(b.value).equals(text)) {
                    return b;
                }
            }
            return null;
        }

    }

    public enum DataRepoRole {
        steward("steward"),
        custodian("custodian"),
        ingester("ingester"),
        reader("reader"),
        discoverer("discoverer");

        private String value;

        DataRepoRole(String value) {
            this.value = value;
        }

        @Override
        @JsonValue
        public String toString() {
            return String.valueOf(value);
        }

        @JsonCreator
        public static ResourceType fromValue(String text) {
            for (ResourceType b : ResourceType.values()) {
                if (String.valueOf(b.value).equals(text)) {
                    return b;
                }
            }
            return null;
        }

        public String getRoleName() {
            return this.value;
        }

        public String getPolicyName() {
            return this.value + "Policy";
        }
    }

    @Value("${sam.basePath}")
    private String samBasePath;

    @Value("${sam.group.email.stewards}")
    private String samStewardsGroupEmail;


    private static Logger logger = LoggerFactory.getLogger(SamClientService.class);

    private ApiClient getApiClient(String accessToken) {
        ApiClient apiClient = new ApiClient();
        apiClient.setAccessToken(accessToken);
        apiClient.setUserAgent("OpenAPI-Generator/1.0.0 java");  // only logs an error in sam
        return apiClient.setBasePath(samBasePath);
    }

    private ResourcesApi samResourcesApi(String accessToken) {
        return new ResourcesApi(getApiClient(accessToken));
    }

    private GoogleApi samGoogleApi(String accessToken) {
        return new GoogleApi(getApiClient(accessToken));
    }

    public boolean checkResourceAction(
        AuthenticatedUserRequest userReq,
        String samResourceType,
        String samResource,
        String action)
            throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getToken());
        return samResourceApi.resourceAction(samResourceType, samResource, action);
    }

    public void deleteStudyResource(AuthenticatedUserRequest userReq, UUID studyId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getToken());
        samResourceApi.deleteResource(ResourceType.study.toString(), studyId.toString());
    }

    public void deleteDatasetResource(AuthenticatedUserRequest userReq, UUID datsetId) throws ApiException {
        ResourcesApi samResourceApi = samResourcesApi(userReq.getToken());
        samResourceApi.deleteResource(ResourceType.dataset.toString(), datsetId.toString());
    }

    public void createResourceForStudy(AuthenticatedUserRequest userReq, UUID studyId) throws ApiException {
        CreateResourceCorrectRequest req = new CreateResourceCorrectRequest();
        req.setResourceId(studyId.toString());
        req.addPoliciesItem(
            DataRepoRole.steward.getPolicyName(),
            createAccessPolicy(DataRepoRole.steward.getRoleName(), samStewardsGroupEmail));
        req.addPoliciesItem(
            DataRepoRole.custodian.getPolicyName(),
            createAccessPolicy(DataRepoRole.custodian.getRoleName(), userReq.getEmail()));
        req.addPoliciesItem(
            DataRepoRole.ingester.getPolicyName(),
            new AccessPolicyMembership().roles(Collections.singletonList(DataRepoRole.ingester.getRoleName())));

        ResourcesApi samResourceApi = samResourcesApi(userReq.getToken());
        logger.debug(req.toString());
        createResourceCorrectCall(samResourceApi.getApiClient(), ResourceType.study.toString(), req);
    }

    public String createResourceForDataset(AuthenticatedUserRequest userReq, UUID datasetId) throws ApiException {
        CreateResourceCorrectRequest req = new CreateResourceCorrectRequest();
        req.setResourceId(datasetId.toString());
        req.addPoliciesItem(
            DataRepoRole.steward.getPolicyName(),
            createAccessPolicy(DataRepoRole.steward.getRoleName(), samStewardsGroupEmail));
        req.addPoliciesItem(
            DataRepoRole.custodian.getPolicyName(),
            createAccessPolicy(DataRepoRole.custodian.getRoleName(), userReq.getEmail()));
        req.addPoliciesItem(
            DataRepoRole.reader.getPolicyName(),
            new AccessPolicyMembership().roles(Collections.singletonList(DataRepoRole.reader.getRoleName())));
        req.addPoliciesItem(
            DataRepoRole.discoverer.getPolicyName(),
            new AccessPolicyMembership().roles(Collections.singletonList(DataRepoRole.discoverer.getRoleName())));

        // create the resource in sam
        ResourcesApi samResourceApi = samResourcesApi(userReq.getToken());
        logger.debug(req.toString());
        createResourceCorrectCall(samResourceApi.getApiClient(), ResourceType.dataset.toString(), req);

        // sync the readers policy
        // Map[WorkbenchEmail, Seq[SyncReportItem]]
        Map<String, List<Object>> results = samGoogleApi(userReq.getToken()).syncPolicy(
            ResourceType.dataset.toString(),
            datasetId.toString(),
            DataRepoRole.reader.getPolicyName());
        return results.keySet().iterator().next();
    }


    private AccessPolicyMembership createAccessPolicy(String role, String email) {
        return new AccessPolicyMembership()
            .roles(Collections.singletonList(role))
            .memberEmails(Collections.singletonList(email));
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

        Object localVarPostBody = resourceCreate;

        // create path and map variables
        String localVarPath = "/api/resources/v1/{resourceTypeName}"
            .replaceAll("\\{" + "resourceTypeName" + "\\}",
                localVarApiClient.escapeString(resourceTypeName.toString()));

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
            localVarPostBody,
            localVarHeaderParams,
            localVarFormParams,
            localVarAuthNames,
            null);
        localVarApiClient.execute(localVarCall);
    }

}

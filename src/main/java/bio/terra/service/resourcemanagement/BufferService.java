package bio.terra.service.resourcemanagement;

import bio.terra.buffer.api.BufferApi;
import bio.terra.buffer.api.UnauthenticatedApi;
import bio.terra.app.configuration.ResourceBufferServiceConfiguration;
import bio.terra.buffer.client.ApiClient;
import bio.terra.buffer.client.ApiException;
import bio.terra.buffer.model.HandoutRequestBody;
import bio.terra.buffer.model.PoolInfo;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.buffer.model.SystemStatus;
import bio.terra.buffer.model.SystemStatusSystems;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.service.resourcemanagement.exception.BufferServiceAPIException;
import bio.terra.service.resourcemanagement.exception.BufferServiceAuthorizationException;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;


/** A service for integrating with the Resource Buffer Service. */
@Component
public class BufferService {
    private final Logger logger = LoggerFactory.getLogger(BufferService.class);

    private final ResourceBufferServiceConfiguration bufferServiceConfiguration;

    @Autowired
    public BufferService(ResourceBufferServiceConfiguration bufferServiceConfiguration) {
        this.bufferServiceConfiguration = bufferServiceConfiguration;
    }

    private ApiClient getApiClient(String accessToken) {
        ApiClient client = new ApiClient();
        client.setAccessToken(accessToken);
        client.setBasePath(bufferServiceConfiguration.getInstanceUrl());
        return client;
    }

    private ApiClient getUnAuthApiClient() {
        ApiClient client = new ApiClient();
        client.setBasePath(bufferServiceConfiguration.getInstanceUrl());
        return client;
    }

    private BufferApi bufferApi(String instanceUrl) throws IOException {
        return new BufferApi(
            getApiClient(bufferServiceConfiguration.getAccessToken()).setBasePath(instanceUrl));
    }

    /**
     * Return the PoolInfo object for the ResourceBuffer pool that we are using to create Google Cloud
     * projects. Note that this is configured once per Workspace Manager instance (both the instance
     * of RBS to use and which pool) so no configuration happens here.
     *
     * @return PoolInfo
     */
    public PoolInfo getPoolInfo() {
        try {
            BufferApi bufferApi = bufferApi(bufferServiceConfiguration.getInstanceUrl());
            PoolInfo info = bufferApi.getPoolInfo(bufferServiceConfiguration.getPoolId());
            logger.info(
                "Retrieved pool {} on Buffer Service instance {}",
                bufferServiceConfiguration.getPoolId(),
                bufferServiceConfiguration.getInstanceUrl());
            return info;
        } catch (IOException e) {
            throw new BufferServiceAuthorizationException(
                "Error reading or parsing credentials file", e.getCause());
        } catch (ApiException e) {
            if (e.getCode() == HttpStatus.UNAUTHORIZED.value()) {
                throw new BufferServiceAuthorizationException(
                    "Not authorized to access Buffer Service", e.getCause());
            } else {
                throw new BufferServiceAPIException(e);
            }
        }
    }

    /**
     * Retrieve a single resource from the Buffer Service. The instance and pool are already
     * configured.
     *
     * @param requestBody
     * @return ResourceInfo
     */
    public ResourceInfo handoutResource(HandoutRequestBody requestBody) {
        try {
            BufferApi bufferApi = bufferApi(bufferServiceConfiguration.getInstanceUrl());
            ResourceInfo info =
                bufferApi.handoutResource(requestBody, bufferServiceConfiguration.getPoolId());
            logger.info(
                "Retrieved resource from pool {} on Buffer Service instance {}",
                bufferServiceConfiguration.getPoolId(),
                bufferServiceConfiguration.getInstanceUrl());
            return info;
        } catch (IOException e) {
            throw new BufferServiceAuthorizationException(
                "Error reading or parsing credentials file", e.getCause());
        } catch (ApiException e) {
            if (e.getCode() == HttpStatus.UNAUTHORIZED.value()) {
                throw new BufferServiceAuthorizationException(
                    "Not authorized to access Buffer Service", e.getCause());
            } else {
                throw new BufferServiceAPIException(e);
            }
        }
    }

    public RepositoryStatusModelSystems status() {
        UnauthenticatedApi unauthenticatedApi = new UnauthenticatedApi(getUnAuthApiClient());
        try {
            SystemStatus status = unauthenticatedApi.serviceStatus();
            Map<String, SystemStatusSystems> subsystemStatusMap =
                status.getSystems();
            return new RepositoryStatusModelSystems()
                .ok(status.isOk())
                .critical(false)
                .message(subsystemStatusMap.toString());
        } catch (ApiException e) {
            return new RepositoryStatusModelSystems()
                .ok(false)
                .critical(false)
                .message(e.getResponseBody());
        }
    }
}

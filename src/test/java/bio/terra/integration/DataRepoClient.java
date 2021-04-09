package bio.terra.integration;

import bio.terra.common.auth.AuthService;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.model.DRSError;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import bio.terra.service.filedata.DrsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static bio.terra.common.TestUtils.mapFromJson;

/**
 * This class holds a Spring RestTemplate
 */
@Component
public class DataRepoClient {
    @Autowired
    private TestConfiguration testConfig;

    @Autowired
    private AuthService authService;

    private static final Logger logger = LoggerFactory.getLogger(DataRepoClient.class);
    private final RestTemplate restTemplate;
    private final HttpHeaders headers;

    public DataRepoClient() {
        restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
        restTemplate.setErrorHandler(new DataRepoClientErrorHandler());

        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON));
    }

    // -- RepositoryController Client --

    private <T> DataRepoResponse<T> makeDataRepoRequest(String path,
                                                        HttpMethod method,
                                                        HttpEntity entity,
                                                        Class<T> responseClass) throws Exception {
        return new DataRepoResponse<T>(makeRequest(path, method, entity, responseClass, ErrorModel.class));
    }

    public <T> DataRepoResponse<T> get(TestConfiguration.User user, String path, Class<T> responseClass)
        throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(getHeaders(user));
        return makeDataRepoRequest(path, HttpMethod.GET, entity, responseClass);
    }

    public <T> DataRepoResponse<T> post(TestConfiguration.User user, String path, String json, Class<T> responseClass)
        throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(json, getHeaders(user));
        return makeDataRepoRequest(path, HttpMethod.POST, entity, responseClass);
    }

    public <T> DataRepoResponse<T> put(TestConfiguration.User user, String path, String json, Class<T> responseClass)
        throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(json, getHeaders(user));
        return makeDataRepoRequest(path, HttpMethod.PUT, entity, responseClass);
    }

    public <T> DataRepoResponse<T> delete(TestConfiguration.User user, String path, Class<T> responseClass)
        throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(getHeaders(user));
        return makeDataRepoRequest(path, HttpMethod.DELETE, entity, responseClass);
    }

    public <T> DataRepoResponse<T> waitForResponseLog(TestConfiguration.User user,
                                                   DataRepoResponse<JobModel> jobModelResponse,
                                                   Class<T> responseClass) throws Exception {
        DataRepoResponse<T> response = waitForResponse(user, jobModelResponse, responseClass);
        // if not successful, log the response
        if (!response.getStatusCode().is2xxSuccessful()) {
            logger.error("operation failed - waiting for " + responseClass.getName());
            if (response.getErrorObject().isPresent()) {
                logger.error("error object: " + response.getErrorObject().get());
            }
        }
        return response;
    }

    public <T> DataRepoResponse<T> waitForResponse(TestConfiguration.User user,
                                                   DataRepoResponse<JobModel> jobModelResponse,
                                                   Class<T> responseClass) throws Exception {

        // if the initial response is bad gateway, then the request probably never got delivered
        if (jobModelResponse.getStatusCode() == HttpStatus.BAD_GATEWAY) {
            throw new IllegalStateException("unexpected job status code: " + jobModelResponse.getStatusCode());
        }

        boolean keepGoing = true;
        String location = getLocationHeader(jobModelResponse);
        while (keepGoing) {
            switch (jobModelResponse.getStatusCode()) {
                case ACCEPTED: {
                    jobModelResponse = waitForResponseUntilNot(user, location, HttpStatus.ACCEPTED);
                    break;
                }

                case BAD_GATEWAY:
                    jobModelResponse = waitForResponseUntilNot(user, location, HttpStatus.BAD_GATEWAY);
                    break;

                default:
                    keepGoing = false;
                    break;
            }
        }

        if (jobModelResponse.getStatusCode() != HttpStatus.OK) {
            throw new IllegalStateException("unexpected job status code: " + jobModelResponse.getStatusCode());
        }

        location = getLocationHeader(jobModelResponse);
        return get(user, location, responseClass);
    }

    // poll for a response with a return status that is not specified status
    private DataRepoResponse<JobModel> waitForResponseUntilNot(TestConfiguration.User user,
                                                               String location,
                                                               HttpStatus notStatus) throws Exception {
        final int initialSeconds = 1;
        final int maxSeconds = 16;
        Instant overTime = Instant.now().plus(Duration.of(1, ChronoUnit.HOURS));

        try {
            int count = 0;
            int sleepSeconds = initialSeconds;

            while (true) {
                logger.info("try #{} until not {} for {}", ++count, notStatus, location);
                TimeUnit.SECONDS.sleep(sleepSeconds);
                sleepSeconds = Math.min(2 * sleepSeconds, maxSeconds);

                DataRepoResponse<JobModel> jobModelResponse = get(user, location, JobModel.class);
                logger.info("Got response. status: " + jobModelResponse.getStatusCode()
                    + " location: " + jobModelResponse.getLocationHeader().orElse("not present"));
                if (jobModelResponse.getStatusCode() != notStatus) {
                    return jobModelResponse;
                }

                if (Instant.now().isAfter(overTime)) {
                    throw new IllegalStateException("we have waited too long for a response");
                }
            }
        } catch (InterruptedException ex) {
            logger.info("interrupted ex: " + ex.getMessage(), ex);
            throw new IllegalStateException("unexpected interrupt waiting for response", ex);
        }
    }

    private String getLocationHeader(DataRepoResponse<JobModel> jobModelResponse) {
        if (!jobModelResponse.getLocationHeader().isPresent()) {
            throw new IllegalStateException("No location header present!");
        }
        return jobModelResponse.getLocationHeader().get();
    }

    // -- DataRepositoryServerController Client --

    /*
     * XXX: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
     * any consequences downstream to DRS clients.
     */
    public <T> DrsResponse<T> drsGet(TestConfiguration.User user, String path, Class<T> responseClass)
        throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(getHeaders(user));
        return makeDrsRequest(path, HttpMethod.GET, entity, responseClass);
    }

    /*
     * XXX: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
     * any consequences downstream to DRS clients.
     */
    private <T> DrsResponse<T> makeDrsRequest(String path,
                                              HttpMethod method,
                                              HttpEntity entity,
                                              Class<T> responseClass) throws Exception {
        return new DrsResponse<T>(makeRequest(path, method, entity, responseClass, DRSError.class));
    }

    // -- Common Client Code --

    private <S, T> ObjectOrErrorResponse<S, T> makeRequest(String path,
                                                           HttpMethod method,
                                                           HttpEntity entity,
                                                           Class<T> responseClass,
                                                           Class<S> errorClass) throws Exception {
        logger.info("api request: method={} path={}", method.toString(), path);

        ResponseEntity<String> response = restTemplate.exchange(
            testConfig.getJadeApiUrl() + path,
            method,
            entity,
            String.class);

        ObjectOrErrorResponse<S, T> drResponse = new ObjectOrErrorResponse<>();
        drResponse.setStatusCode(response.getStatusCode());

        URI uri = response.getHeaders().getLocation();
        drResponse.setLocationHeader((uri == null) ? Optional.empty() : Optional.of(uri.toString()));

        if (response.getStatusCode().is2xxSuccessful()) {
            if (responseClass != null) {
                T responseObject = mapFromJson(response.getBody(), responseClass);
                drResponse.setResponseObject(Optional.of(responseObject));
            } else {
                drResponse.setResponseObject(Optional.empty());
            }
            drResponse.setErrorModel(Optional.empty());
        } else {
            S errorObject = mapFromJson(response.getBody(), errorClass);
            drResponse.setErrorModel(Optional.of(errorObject));
            drResponse.setResponseObject(Optional.empty());
        }

        return drResponse;
    }

    private HttpHeaders getHeaders(TestConfiguration.User user) {
        HttpHeaders copy = new HttpHeaders(headers);
        copy.setBearerAuth(authService.getAuthToken(user.getEmail()));
        copy.set("From", user.getEmail());
        return copy;
    }

}

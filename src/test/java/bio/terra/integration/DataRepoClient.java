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

import java.io.IOException;
import java.net.URI;
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

    private static Logger logger = LoggerFactory.getLogger(DataRepoClient.class);
    private RestTemplate restTemplate;
    private HttpHeaders headers;

    public DataRepoClient() {
        restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
        restTemplate.setErrorHandler(new DataRepoClientErrorHandler());

        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON_UTF8));
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

    public boolean pollForResponse(TestConfiguration.User user,
                                   DataRepoResponse<JobModel> jobModelResponse,
                                   int pollTime,
                                   int retryCount) throws Exception {
        IOException lastException = null;
        for(int i = 0; i < retryCount; i++) {
            int totalSleep = 0;
            final int initialSeconds = 1;
            final int maxSeconds = 16;

            try {
                int count = 0;
                int sleepSeconds = initialSeconds;
                while (jobModelResponse.getStatusCode() == HttpStatus.ACCEPTED) {
                    if (totalSleep >= pollTime) {
                        return false;
                    }
                    String location = getLocationHeader(jobModelResponse);
                    logger.info("try #{} for {}", ++count, location);


                    TimeUnit.SECONDS.sleep(sleepSeconds);
                    totalSleep += sleepSeconds;

                    jobModelResponse = get(user, location, JobModel.class);

                    int nextSeconds = 2 * sleepSeconds;
                    sleepSeconds = (nextSeconds > maxSeconds) ? maxSeconds : nextSeconds;
                }
                return true;
            } catch (IOException ex) {
                logger.info("unable to load json");
                lastException = ex;
                TimeUnit.SECONDS.sleep(30);
            } catch (Exception ex) {
                logger.info("interrupted ex: {}", ex.getMessage());
                ex.printStackTrace();
                Thread.currentThread().interrupt();
                throw new IllegalStateException("unexpected interrupt waiting for response", ex);
            }
        }
        throw lastException;
    }

    public <T> DataRepoResponse<T> waitForResponse(TestConfiguration.User user,
                                                   DataRepoResponse<JobModel> jobModelResponse,
                                                   Class<T> responseClass) throws Exception {
        final int initialSeconds = 1;
        final int maxSeconds = 16;
        IOException lastException = null;
        for (int i = 0; i < 4; i++) {
            try {
                int count = 0;
                int sleepSeconds = initialSeconds;
                while (jobModelResponse.getStatusCode() == HttpStatus.ACCEPTED) {
                    String location = getLocationHeader(jobModelResponse);
                    logger.info("try #{} for {}", ++count, location);

                    TimeUnit.SECONDS.sleep(sleepSeconds);
                    jobModelResponse = get(user, location, JobModel.class);

                    int nextSeconds = 2 * sleepSeconds;
                    sleepSeconds = (nextSeconds > maxSeconds) ? maxSeconds : nextSeconds;
                }
            } catch (IOException ex) {
                logger.info("Try #{}: unable to load json. Retrying.", i + 1);
                lastException = ex;
                TimeUnit.SECONDS.sleep(30);
            } catch (InterruptedException ex) {
                logger.info("interrupted ex: {}", ex.getMessage());
                ex.printStackTrace();
                Thread.currentThread().interrupt();
                throw new IllegalStateException("unexpected interrupt waiting for response", ex);
            }
        }

        if (jobModelResponse.getStatusCode() != HttpStatus.OK) {
            if (lastException != null){
                throw lastException;
            }
            throw new IllegalStateException("unexpected job status code: " + jobModelResponse.getStatusCode());
        }

        String location = getLocationHeader(jobModelResponse);
        DataRepoResponse<T> resultResponse = get(user, location, responseClass);

        return resultResponse;
    }

    private String getLocationHeader(DataRepoResponse<JobModel> jobModelResponse) {
        if (!jobModelResponse.getLocationHeader().isPresent()) {
            throw new IllegalStateException("No location header present!");
        }
        return jobModelResponse.getLocationHeader().get();
    }

    // -- DataRepositoryServerController Client --

    public <T> DrsResponse<T> drsGet(TestConfiguration.User user, String path, Class<T> responseClass)
        throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(getHeaders(user));
        return makeDrsRequest(path, HttpMethod.GET, entity, responseClass);
    }

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

        String url = testConfig.getJadeApiUrl() + path;
        logger.info("api request: method={} path={}", method.toString(), url);

        ResponseEntity<String> response = restTemplate.exchange(
            url,
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

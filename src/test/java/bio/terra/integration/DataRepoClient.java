package bio.terra.integration;

import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
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
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * This class holds a Spring RestTemplate
 */
@Component
@Profile("integrationtest")
public class DataRepoClient {
    @Autowired
    private TestConfiguration testConfig;

    private static Logger logger = LoggerFactory.getLogger(DataRepoClient.class);
    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;
    private HttpHeaders headers;

    public DataRepoClient() {
        restTemplate = new RestTemplate();
        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
//        restTemplate.setErrorHandler(new DefaultResponseErrorHandler() { public boolean hasError(ClientHttpResponse response) throws IOException { HttpStatus statusCode = response.getStatusCode(); return statusCode.series() == HttpStatus.Series.SERVER_ERROR; } });
        restTemplate.setErrorHandler(new DataRepoClientErrorHandler());
        objectMapper = new ObjectMapper();

        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON_UTF8));
    }

    private HttpHeaders getHeaders(String authToken) {
        HttpHeaders copy = new HttpHeaders(headers);
        copy.setBearerAuth(authToken);
        return copy;
    }

    public <T> DataRepoResponse<T> get(String authToken, String path, Class<T> responseClass) throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(getHeaders(authToken));
        return makeDataRepoRequest(path, HttpMethod.GET, entity, responseClass);
    }

    public <T> DataRepoResponse<T> post(String authToken, String path, String json, Class<T> responseClass)
        throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(json, getHeaders(authToken));
        return makeDataRepoRequest(path, HttpMethod.POST, entity, responseClass);
    }

    public <T> DataRepoResponse<T> delete(String authToken, String path, Class<T> responseClass) throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(getHeaders(authToken));
        return makeDataRepoRequest(path, HttpMethod.DELETE, entity, responseClass);
    }

    public <T> DataRepoResponse<T> waitForResponse(String authToken,
                                                   DataRepoResponse<JobModel> jobModelResponse,
                                                   Class<T> responseClass) throws Exception {
        try {
            while (jobModelResponse.getStatusCode() == HttpStatus.ACCEPTED) {
                String location = getLocationHeader(jobModelResponse);

                // TODO: tune this. Maybe use exponential backoff?
                TimeUnit.SECONDS.sleep(10);
                jobModelResponse = get(authToken, location, JobModel.class);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("unexpected interrupt waiting for response");
        }

        if (jobModelResponse.getStatusCode() != HttpStatus.OK) {
            throw new IllegalStateException("unexpected interrupt waiting for response");
        }

        String location = getLocationHeader(jobModelResponse);
        DataRepoResponse<T> resultResponse = get(authToken, location, responseClass);

        return resultResponse;
    }

    private String getLocationHeader(DataRepoResponse<JobModel> jobModelResponse) {
        if (!jobModelResponse.getLocationHeader().isPresent()) {
            throw new IllegalStateException("No location header present!");
        }
        return jobModelResponse.getLocationHeader().get();
    }

    private <T> DataRepoResponse<T> makeDataRepoRequest(String path,
                                                        HttpMethod method,
                                                        HttpEntity entity,
                                                        Class<T> responseClass) throws Exception {

        ResponseEntity<String> response = restTemplate.exchange(
            testConfig.getJadeApiUrl() + path,
            method,
            entity,
            String.class);

        DataRepoResponse<T> drResponse = new DataRepoResponse<>();
        drResponse.setStatusCode(response.getStatusCode());

        URI uri = response.getHeaders().getLocation();
        drResponse.setLocationHeader((uri == null) ? Optional.empty() : Optional.of(uri.toString()));

        if (response.getStatusCode().is2xxSuccessful()) {
            if (responseClass != null) {
                T responseObject = objectMapper.readValue(response.getBody(), responseClass);
                drResponse.setResponseObject(Optional.of(responseObject));
            } else {
                drResponse.setResponseObject(Optional.empty());
            }
            drResponse.setErrorModel(Optional.empty());
        } else {
            ErrorModel errorModel = objectMapper.readValue(response.getBody(), ErrorModel.class);
            drResponse.setErrorModel(Optional.of(errorModel));
            drResponse.setResponseObject(Optional.empty());
        }

        return drResponse;
    }

}

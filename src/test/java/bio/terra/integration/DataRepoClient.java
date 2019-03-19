package bio.terra.integration;

import bio.terra.model.DeleteResponseModel;
import bio.terra.model.ErrorModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.Optional;

/**
 * This class holds a Spring RestTemplate
 */
@Component
public class DataRepoClient {
    @Value("${integrationtest.port}")
    private String testPort;

    @Value("${integrationtest.server}")
    private String testServer;

    @Value("${integrationtest.protocol}")
    private String testProtocol;

    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;
    private HttpHeaders headers;

    public DataRepoClient() {
        restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(new DataRepoClientErrorHandler());
        objectMapper = new ObjectMapper();

        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON_UTF8));
    }

    public <T> DataRepoResponse<T> get(String path, Class<T> responseClass) throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(headers);
        return makeDataRepoRequest(path, HttpMethod.GET, entity, responseClass);
    }

    public <T> DataRepoResponse<T> post(String path, String json, Class<T> responseClass) throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(json, headers);
        return makeDataRepoRequest(path, HttpMethod.POST, entity, responseClass);
    }

    public DataRepoResponse<DeleteResponseModel> delete(String path) throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(headers);
        return makeDataRepoRequest(path, HttpMethod.DELETE, entity, DeleteResponseModel.class);
    }

    private <T> DataRepoResponse<T> makeDataRepoRequest(String path,
                                                        HttpMethod method,
                                                        HttpEntity entity,
                                                        Class<T> responseClass) throws Exception {

        ResponseEntity<String> response = restTemplate.exchange(
            makeUrl(path),
            method,
            entity,
            String.class);

        DataRepoResponse<T> drResponse = new DataRepoResponse<>();
        drResponse.setStatusCode(response.getStatusCode());

        if (response.getStatusCode().is2xxSuccessful()) {
            T responseObject = objectMapper.readValue(response.getBody(), responseClass);
            drResponse.setResponseObject(Optional.of(responseObject));
            drResponse.setErrorModel(Optional.empty());
        } else {
            ErrorModel errorModel = objectMapper.readValue(response.getBody(), ErrorModel.class);
            drResponse.setErrorModel(Optional.of(errorModel));
            drResponse.setResponseObject(Optional.empty());
        }

        return drResponse;
    }

    private String makeUrl(String path) {
        return String.format("%s://%s:%s%s", testProtocol, testServer, testPort, path);
    }
}

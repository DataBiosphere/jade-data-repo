package bio.terra.integration;

import bio.terra.SwaggerDocumentationConfig;
import bio.terra.model.ErrorModel;
import bio.terra.model.JobModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * This class holds a Spring RestTemplate
 */
@Component
public class DataRepoClient {
    private DataRepoConfiguration dataRepoConfiguration;
    private SwaggerDocumentationConfig swaggerConfig;

    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;
    private HttpHeaders headers;
    private String stewardAccessToken;

    @Autowired
    public DataRepoClient(
        DataRepoConfiguration dataRepoConfiguration,
        SwaggerDocumentationConfig swaggerConfig
    ) throws Exception {
        this.dataRepoConfiguration = dataRepoConfiguration;
        this.swaggerConfig = swaggerConfig;
        // configure the http client to work with ssl but not verify certs
        TrustStrategy acceptingTrustStrategy = (X509Certificate[] chain, String authType) -> true;
        SSLContext sslContext = org.apache.http.ssl.SSLContexts.custom()
                .loadTrustMaterial(null, acceptingTrustStrategy)
                .build();
        SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext);
        CloseableHttpClient httpClient = HttpClients.custom()
                .setSSLSocketFactory(csf)
                .build();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setHttpClient(httpClient);
        restTemplate = new RestTemplate(requestFactory);
        restTemplate.setErrorHandler(new DataRepoClientErrorHandler());
        objectMapper = new ObjectMapper();
        stewardAccessToken = refresh(dataRepoConfiguration.getStewardRefreshToken());
        headers = new HttpHeaders();
        headers.setBearerAuth(stewardAccessToken);
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

    public <T> DataRepoResponse<T> delete(String path, Class<T> responseClass) throws Exception {
        HttpEntity<String> entity = new HttpEntity<>(headers);
        return makeDataRepoRequest(path, HttpMethod.DELETE, entity, responseClass);
    }

    public <T> DataRepoResponse<T> waitForResponse(DataRepoResponse<JobModel> jobModelResponse,
                                                   Class<T> responseClass) throws Exception {
        try {
            while (jobModelResponse.getStatusCode() == HttpStatus.ACCEPTED) {
                String location = getLocationHeader(jobModelResponse);

                // TODO: tune this. Maybe use exponential backoff?
                TimeUnit.SECONDS.sleep(10);
                jobModelResponse = get(location, JobModel.class);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("unexpected interrupt waiting for response");
        }

        if (jobModelResponse.getStatusCode() != HttpStatus.OK) {
            throw new IllegalStateException("unexpected interrupt waiting for response");
        }

        String location = getLocationHeader(jobModelResponse);
        DataRepoResponse<T> resultResponse = get(location, responseClass);

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
            makeUrl(path),
            method,
            entity,
            String.class);

        DataRepoResponse<T> drResponse = new DataRepoResponse<>();
        drResponse.setStatusCode(response.getStatusCode());

        URI uri = response.getHeaders().getLocation();
        drResponse.setLocationHeader((uri == null) ? Optional.empty() : Optional.of(uri.toString()));

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
        return String.format("%s://%s:%s%s",
            dataRepoConfiguration.getProtocol(),
            dataRepoConfiguration.getServer(),
            dataRepoConfiguration.getPort(),
            path);
    }

    private String refresh(String refreshToken) {
        String url = "https://www.googleapis.com/oauth2/v4/token";
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("refresh_token", refreshToken);
        map.add("client_id", swaggerConfig.getClientId());
        map.add("client_secret", swaggerConfig.getClientSecret());
        map.add("grant_type", "refresh_token");

        headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        HttpEntity<MultiValueMap<String, String>> entity = new HttpEntity<>(map, headers);

        ResponseEntity<RefreshTokenResponse> response = restTemplate.postForEntity(url, entity,
            RefreshTokenResponse.class);
        return response.getBody().getAccessToken();
    }
}

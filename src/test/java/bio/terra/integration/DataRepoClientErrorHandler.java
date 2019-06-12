package bio.terra.integration;

import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;

import java.io.IOException;

// Error handler that does nothing, so we can let the tests process the status code as they wish
public class DataRepoClientErrorHandler implements ResponseErrorHandler {
    @Override
    public boolean hasError(ClientHttpResponse response) throws IOException {
        return false;
//        HttpStatus statusCode = response.getStatusCode();
//        return statusCode.series() == HttpStatus.Series.SERVER_ERROR;
    }

    @Override
    public void handleError(ClientHttpResponse response) throws IOException {

    }
}

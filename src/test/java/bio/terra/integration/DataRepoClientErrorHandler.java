package bio.terra.integration;

import java.net.URI;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;

// Error handler that does nothing, so we can let the tests process the status code as they wish
public class DataRepoClientErrorHandler implements ResponseErrorHandler {
  @Override
  public boolean hasError(ClientHttpResponse response) {
    return false;
  }

  @Override
  public void handleError(URI url, HttpMethod method, ClientHttpResponse response) {
    // do nothing
  }
}

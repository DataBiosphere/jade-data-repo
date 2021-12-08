package bio.terra.service.resourcemanagement.google;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CloudResourceManagerService {
  private static final Logger logger = LoggerFactory.getLogger(CloudResourceManagerService.class);

  private final GoogleResourceConfiguration resourceConfiguration;

  @Autowired
  public CloudResourceManagerService(GoogleResourceConfiguration resourceConfiguration) {
    this.resourceConfiguration = resourceConfiguration;
  }

  // TODO: convert this to using the resource manager service interface instead of the api interface
  //  https://googleapis.dev/java/google-cloud-resourcemanager/latest/index.html
  //     ?com/google/cloud/resourcemanager/ResourceManager.html
  //  And use GoogleCredentials instead of the deprecated class. (DR-1459)
  public CloudResourceManager cloudResourceManager() throws IOException, GeneralSecurityException {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    return new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
        .setApplicationName(resourceConfiguration.getApplicationName())
        .build();
  }
}

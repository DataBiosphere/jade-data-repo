package bio.terra.service.notification;

import bio.terra.app.configuration.NotificationConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {

  private final PubSubService pubSubService;
  private final NotificationConfiguration notificationConfiguration;
  private final ObjectMapper objectMapper;

  public NotificationService(
      PubSubService pubSubService,
      NotificationConfiguration notificationConfiguration,
      ObjectMapper objectMapper) {
    this.pubSubService = pubSubService;
    this.notificationConfiguration = notificationConfiguration;
    this.objectMapper = objectMapper;
  }

  @VisibleForTesting
  protected void createTopic() {
    try {
      pubSubService.createTopic(
          notificationConfiguration.projectId(), notificationConfiguration.topicId());
    } catch (IOException e) {
      throw new NotificationException("Error creating notification topic", e);
    }
  }

  public void snapshotReady(
      AuthenticatedUserRequest user,
      String snapshotExportLink,
      String snapshotName,
      String snapshotSummary) {
    try {
      pubSubService.publishMessage(
          notificationConfiguration.projectId(),
          notificationConfiguration.topicId(),
          objectMapper.writeValueAsString(
              new SnapshotReadyNotification(
                  user.getSubjectId(), snapshotExportLink, snapshotName, snapshotSummary)));
    } catch (IOException e) {
      throw new NotificationException("Error sending notification", e);
    }
  }
}

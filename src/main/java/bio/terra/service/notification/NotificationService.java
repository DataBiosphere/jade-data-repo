package bio.terra.service.notification;

import bio.terra.app.configuration.NotificationConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.rpc.ApiException;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class NotificationService {

  private static final Logger logger = LoggerFactory.getLogger(NotificationService.class);

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
  @PostConstruct
  protected void createTopic() {
    try {
      pubSubService.createTopic(
          notificationConfiguration.projectId(), notificationConfiguration.topicId());
    } catch (IOException | ApiException e) {
      logger.warn("Error creating notification topic", e);
    }
  }

  public void snapshotReady(
      String subjectId, String snapshotExportLink, String snapshotName, String snapshotSummary) {
    try {
      pubSubService.publishMessage(
          notificationConfiguration.projectId(),
          notificationConfiguration.topicId(),
          objectMapper.writeValueAsString(
              new SnapshotReadyNotification(
                  subjectId, snapshotExportLink, snapshotName, snapshotSummary)));
    } catch (IOException | ApiException e) {
      logger.warn("Error sending notification", e);
    }
  }
}

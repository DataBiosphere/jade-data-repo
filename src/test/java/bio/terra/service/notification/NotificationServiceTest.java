package bio.terra.service.notification;

import static org.mockito.Mockito.verify;

import bio.terra.app.configuration.NotificationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {
  @Mock private PubSubService pubSubService;
  private NotificationService notificationService;
  private final NotificationConfiguration configuration =
      new NotificationConfiguration("project", "topic");

  @BeforeEach
  void beforeEach() {
    notificationService = new NotificationService(pubSubService, configuration, new ObjectMapper());
  }

  @Test
  void createTopic() throws Exception {
    notificationService.createTopic();
    verify(pubSubService).createTopic(configuration.projectId(), configuration.topicId());
  }

  @Test
  void snapshotReady() throws Exception {
    var user = AuthenticationFixtures.randomUserRequest();
    notificationService.snapshotReady(user, "link", "name", "summary");
    verify(pubSubService)
        .publishMessage(
            configuration.projectId(),
            configuration.topicId(),
            """
            {"notificationType":"SnapshotReadyNotification","recipientUserId":"subjectid","snapshotExportLink":"link","snapshotName":"name","snapshotSummary":"summary"}""");
  }
}

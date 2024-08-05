package bio.terra.service.notification;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class PubSubService {
  private static final Logger logger = LoggerFactory.getLogger(PubSubService.class);

  public void createTopic(String projectId, String topicId) throws IOException {
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      TopicName topicName = TopicName.of(projectId, topicId);
      Topic topic = topicAdminClient.createTopic(topicName);
      logger.info("Created topic: {}", topic.getName());
    }
  }

  public void publishMessage(String projectId, String topicId, String message) throws IOException {
    TopicName topicName = TopicName.of(projectId, topicId);
    var publisher = Publisher.newBuilder(topicName).build();
    publisher.publish(PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message)).build());
    logger.info("Published message ID: {}", message);
  }
}

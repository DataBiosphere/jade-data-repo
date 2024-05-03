package bio.terra.service.rawls;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class WorkspaceDetailsTest {
  private static final String WORKSPACE_ID = UUID.randomUUID().toString();
  private static final String NAMESPACE = "namespace";
  private static final String NAME = "name";

  @Test
  void workspaceDetails() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    String content =
        """
        {"workspaceId": "%s", "namespace": "%s", "name": "%s", "unknown": "property"}"""
            .formatted(WORKSPACE_ID, NAMESPACE, NAME);
    assertThat(
        objectMapper.readValue(content, WorkspaceDetails.class),
        equalTo(new WorkspaceDetails(WORKSPACE_ID, NAMESPACE, NAME)));
  }
}

package bio.terra.common.fixtures;

import bio.terra.model.DuosFirecloudGroupModel;
import java.time.Instant;
import java.util.UUID;

public final class DuosFixtures {
  private DuosFixtures() {}

  public static DuosFirecloudGroupModel duosFirecloudGroupCreated(String duosId) {
    String firecloudGroupName = String.format("%s-users", duosId);
    String firecloudGroupEmail = String.format("%s@dev.test.firecloud.org", firecloudGroupName);

    return new DuosFirecloudGroupModel()
        .duosId(duosId)
        .firecloudGroupName(firecloudGroupName)
        .firecloudGroupEmail(firecloudGroupEmail);
  }

  public static DuosFirecloudGroupModel duosFirecloudGroupFromDb(String duosId, UUID id) {
    return duosFirecloudGroupCreated(duosId)
        .id(id)
        .created(Instant.now().toString())
        .createdBy("jade-k8-sa@broad-jade-dev.iam.gserviceaccount.com");
  }

  public static DuosFirecloudGroupModel duosFirecloudGroupFromDb(String duosId) {
    return duosFirecloudGroupFromDb(duosId, UUID.randomUUID());
  }
}

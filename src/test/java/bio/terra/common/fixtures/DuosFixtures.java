package bio.terra.common.fixtures;

import bio.terra.model.DuosFirecloudGroupModel;
import java.time.Instant;
import java.util.UUID;

public final class DuosFixtures {

  /**
   * @param duosId DUOS dataset ID
   * @return a DuosFirecloudGroupModel created from the DUOS ID, populated as if returned by the
   *     DUOS Firecloud group creation process. No external calls are made.
   */
  public static DuosFirecloudGroupModel createFirecloudGroup(String duosId) {
    String firecloudGroupName = String.format("%s-users", duosId);
    String firecloudGroupEmail = String.format("%s@dev.test.firecloud.org", firecloudGroupName);

    return new DuosFirecloudGroupModel()
        .duosId(duosId)
        .firecloudGroupName(firecloudGroupName)
        .firecloudGroupEmail(firecloudGroupEmail);
  }

  /**
   * @param duosId DUOS dataset ID
   * @return a DuosFirecloudGroupModel created from the DUOS ID, populated as if returned via
   *     database retrieval. No external calls are made.
   */
  public static DuosFirecloudGroupModel createDbFirecloudGroup(String duosId) {
    return createFirecloudGroup(duosId)
        .id(UUID.randomUUID())
        .created(Instant.now().toString())
        .createdBy("jade-k8-sa@broad-jade-dev.iam.gserviceaccount.com");
  }

  /**
   * @param duosId DUOS dataset ID
   * @return a DuosFirecloudGroupModel created from the DUOS ID, populated as if synced and returned
   *     via database retrieval. No external calls are made.
   */
  public static DuosFirecloudGroupModel createDbSyncedFirecloudGroup(String duosId) {
    return createDbFirecloudGroup(duosId).lastSynced(Instant.now().toString());
  }
}

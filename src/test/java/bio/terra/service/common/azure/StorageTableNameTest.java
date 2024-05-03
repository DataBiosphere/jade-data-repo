package bio.terra.service.common.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class StorageTableNameTest {

  @Test
  void snapshotToTableNameWithParam() {
    UUID snapshotId = UUID.randomUUID();
    String expectedTableName = "datarepo" + snapshotId.toString().replaceAll("-", "") + "snapshot";
    String snapshotTableName = StorageTableName.SNAPSHOT.toTableName(snapshotId);
    assertThat(
        "expected snapshot table name should match", snapshotTableName, equalTo(expectedTableName));
  }
}

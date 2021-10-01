package bio.terra.service.common.azure;

import static bio.terra.service.common.azure.StorageTableName.DATASET_TABLE;
import static bio.terra.service.common.azure.StorageTableName.SNAPSHOT_TABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotImplementedException;
import java.security.InvalidParameterException;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class StorageTableNameTest {

  @Test(expected = InvalidParameterException.class)
  public void snapshotToTableNameNoParam() {
    SNAPSHOT_TABLE.toTableName();
  }

  @Test
  public void snapshotToTableNameWithParam() {
    UUID snapshotId = UUID.randomUUID();
    String expectedTableName = "datarepo" + snapshotId.toString().replaceAll("-", "") + "snapshot";
    String snapshotTableName = SNAPSHOT_TABLE.toTableName(snapshotId);
    assertThat(
        "expected snapshot table name should match", snapshotTableName, equalTo(expectedTableName));
  }

  @Test
  public void datasetToTableNameNoParam() {
    String datasetTableName = DATASET_TABLE.toTableName();
    assertThat("expected snapshot table name should match", datasetTableName, equalTo("dataset"));
  }

  @Test(expected = NotImplementedException.class)
  public void datasetToTableNameWithParam() {
    DATASET_TABLE.toTableName(UUID.randomUUID());
  }
}

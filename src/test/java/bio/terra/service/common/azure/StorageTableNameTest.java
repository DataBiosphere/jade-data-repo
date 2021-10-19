package bio.terra.service.common.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.NotImplementedException;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class StorageTableNameTest {

  @Test(expected = IllegalArgumentException.class)
  public void snapshotToTableNameNoParam() {
    StorageTableName.SNAPSHOT.toTableName();
  }

  @Test
  public void snapshotToTableNameWithParam() {
    UUID snapshotId = UUID.randomUUID();
    String expectedTableName = "datarepo" + snapshotId.toString().replaceAll("-", "") + "snapshot";
    String snapshotTableName = StorageTableName.SNAPSHOT.toTableName(snapshotId);
    assertThat(
        "expected snapshot table name should match", snapshotTableName, equalTo(expectedTableName));
  }

  @Test
  public void datasetToTableNameNoParam() {
    String datasetTableName = StorageTableName.DATASET.toTableName();
    assertThat("expected snapshot table name should match", datasetTableName, equalTo("dataset"));
  }

  @Test(expected = NotImplementedException.class)
  public void datasetToTableNameWithParam() {
    StorageTableName.DATASET.toTableName(UUID.randomUUID());
  }
}

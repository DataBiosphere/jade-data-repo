package bio.terra.service.load;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;

import bio.terra.common.category.Unit;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class LoadLockKeyTest {
  private static final String LOAD_TAG = "loadTag";
  private static final UUID DATASET_ID = UUID.randomUUID();

  @Test
  void loadLockKey() {
    LoadLockKey loadLockKey = new LoadLockKey(LOAD_TAG, DATASET_ID);
    assertThat(loadLockKey.loadTag(), equalTo(LOAD_TAG));
    assertThat(loadLockKey.datasetId(), equalTo(DATASET_ID));
  }

  @Test
  void loadLockKey_noLoadTag() {
    LoadLockKey loadLockKey = new LoadLockKey(DATASET_ID);
    assertNull(loadLockKey.loadTag());
    assertThat(loadLockKey.datasetId(), equalTo(DATASET_ID));
  }
}

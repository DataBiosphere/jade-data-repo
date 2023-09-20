package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import bio.terra.common.category.Unit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class MapUtilsTest {

  private static final Map<String, Integer> BASE_MAP =
      Map.of(
          "zero", 0,
          "one", 1,
          "two", 2);

  @Test
  public void testPartitionMap() {
    List<Map<String, Integer>> partitionedMap = MapUtils.partitionMap(BASE_MAP, 2);
    assertThat("two chunks were created", partitionedMap, hasSize(2));
    Map<String, Integer> reconstitutedMap = new HashMap<>();
    partitionedMap.forEach(reconstitutedMap::putAll);
    assertThat(
        "can reconstitute the partitioned map and it equals the original",
        reconstitutedMap,
        equalTo(BASE_MAP));
  }

  @Test
  public void testPartitionMapBiggerChunkSize() {
    List<Map<String, Integer>> partitionedMap = MapUtils.partitionMap(BASE_MAP, 20);
    assertThat("one chunk was created", partitionedMap, hasSize(1));
    assertThat(
        "can reconstitute the partitioned map and it equals the original",
        partitionedMap.get(0),
        equalTo(BASE_MAP));
  }

  @Test
  public void testPartitionMapEmptyMap() {
    List<Map<String, Integer>> partitionedMap = MapUtils.partitionMap(Map.of(), 10);
    assertThat("no chunks were created", partitionedMap, hasSize(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPartitionMapInvalidChunkSize() {
    MapUtils.partitionMap(BASE_MAP, -1);
  }
}

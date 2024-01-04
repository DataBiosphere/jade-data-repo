package bio.terra.common;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.arrow.util.Preconditions;
import org.apache.commons.collections4.ListUtils;

public class MapUtils {

  private MapUtils() {}

  /**
   * Partition a map into chunks of maximum size
   *
   * @param map The map to partition
   * @param chunkSize The maximum size of the partitions
   * @return A list of partitioned maps
   */
  public static <K, V> List<Map<K, V>> partitionMap(Map<K, V> map, int chunkSize) {
    Preconditions.checkArgument(chunkSize > 0, "chunk size must be greater than zero");
    return ListUtils.partition(List.copyOf(map.entrySet()), chunkSize).stream()
        .map(l -> l.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue)))
        .toList();
  }
}

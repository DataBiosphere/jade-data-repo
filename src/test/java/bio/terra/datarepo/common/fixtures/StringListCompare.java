package bio.terra.datarepo.common.fixtures;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// StringListCompare compares two lists to make sure that all values in list1 exist in list2
// the same number of times.
public class StringListCompare {
  private List<String> list1;
  private List<String> list2;
  private Map<String, Integer> list1Matches;
  private Map<String, Integer> list2Matches;

  public StringListCompare(List<String> list1, List<String> list2) {
    this.list1 = list1;
    this.list2 = list2;
    this.list1Matches = new HashMap<>();
    this.list2Matches = new HashMap<>();
  }

  public boolean compare() {
    // Handle nulls - if both are null, we call that equal
    if (list1 == null) {
      return (list2 == null);
    }
    if (list2 == null) {
      return false;
    }

    // Size test
    int allSize = list1.size();
    if (allSize != list2.size()) {
      return false;
    }

    // Build a map of each list (eliminating duplicate entries)
    for (int i = 0; i < allSize; i++) {
      list1Matches.put(list1.get(i), 0);
      list2Matches.put(list2.get(i), 0);
    }

    // Driving with one list, make sure each item exists in the other list and count the frequency
    // We bail if an item is not found in one direction or the other
    if (!countMatches(list1, list2Matches)) {
      return false;
    }
    if (!countMatches(list2, list1Matches)) {
      return false;
    }

    // Make sure the frequencies match
    for (Map.Entry<String, Integer> entry1 : list1Matches.entrySet()) {
      int value2 = list2Matches.get(entry1.getKey());
      if (entry1.getValue() != value2) {
        return false;
      }
    }
    return true;
  }

  private boolean countMatches(List<String> list, Map<String, Integer> map) {
    for (int i = 0; i < list.size(); i++) {
      String item1 = list.get(i);
      if (map.containsKey(item1)) {
        int current = map.get(item1);
        map.put(item1, current + 1);
      } else {
        return false;
      }
    }
    return true;
  }
}

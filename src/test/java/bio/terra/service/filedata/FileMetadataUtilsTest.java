package bio.terra.service.filedata;

import static bio.terra.service.filedata.FileMetadataUtils.ROOT_DIR_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections4.map.LRUMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class FileMetadataUtilsTest {

  @Test
  public void makeLookupPathNoRootDir() {
    String noRootDir = "/test/path/file.json";
    String lookupPath = FileMetadataUtils.makeLookupPath(noRootDir);
    assertThat(
        "Root directory should be prepended", lookupPath, equalTo(ROOT_DIR_NAME + noRootDir));
  }

  @Test
  public void makeLookupPathWithRootDir() {
    String withRootDir = ROOT_DIR_NAME + "/test/path/file.json";
    String lookupPath = FileMetadataUtils.makeLookupPath(withRootDir);
    assertThat("url should not have changed.", lookupPath, equalTo(withRootDir));
  }

  @Test
  public void findNewDirectoryPaths() {
    List<FireStoreDirectoryEntry> testEntries = initTestEntries(2);
    // LRU has enough space for all entries
    LRUMap<String, Boolean> pathMap = new LRUMap<>(5);

    Set<String> newPaths = FileMetadataUtils.findNewDirectoryPaths(testEntries, pathMap);
    assertThat("Should be 3 new entries", newPaths.size(), equalTo(3));
    assertThat(
        "Should contain 3 unique directory paths",
        newPaths,
        containsInAnyOrder("/_dr_/test/path-0", "/_dr_/test", "/_dr_/test/path-1"));
    assertThat("Path Map contains each item", pathMap, hasKey("/_dr_/test/path-0"));
    assertThat("Path Map contains each item", pathMap, hasKey("/_dr_/test"));
    assertThat("Path Map contains each item", pathMap, hasKey("/_dr_/test/path-1"));

    // Make same call - should return zero new paths. Path map should not change
    Set<String> zeroNewPaths = FileMetadataUtils.findNewDirectoryPaths(testEntries, pathMap);
    assertThat(
        "There should not be any new paths after making the same call",
        zeroNewPaths.size(),
        equalTo(0));
    assertThat("Path Map should still contain each item", pathMap, hasKey("/_dr_/test/path-0"));
    assertThat("Path Map should still contain each item", pathMap, hasKey("/_dr_/test"));
    assertThat("Path Map should still contain each item", pathMap, hasKey("/_dr_/test/path-1"));
  }

  @Test
  public void findNewDirectoryPathsTestCache() {
    List<FireStoreDirectoryEntry> testEntries = initTestEntries(5);
    // LRU has enough space for all entries
    LRUMap<String, Boolean> pathMap = new LRUMap<>(2);

    Set<String> newPaths = FileMetadataUtils.findNewDirectoryPaths(testEntries, pathMap);
    assertThat("Should be 6 new entries", newPaths.size(), equalTo(6));
    assertThat(
        "Should contain 6 unique directory paths",
        newPaths,
        containsInAnyOrder(
            "/_dr_/test/path-0",
            "/_dr_/test",
            "/_dr_/test/path-1",
            "/_dr_/test/path-2",
            "/_dr_/test/path-3",
            "/_dr_/test/path-4"));
    assertThat(
        "With LRU size = 2, Path map would contain the last unique path added",
        pathMap,
        hasKey("/_dr_/test/path-4"));
    assertThat(
        "With LRU size = 2, Path map would contain the common path between directories",
        pathMap,
        hasKey("/_dr_/test"));

    // Make same call - with one added path. All but the common path was kicked out the cache, so
    // we'll see
    // 6 unique entries
    testEntries.add(new FireStoreDirectoryEntry().path("/test/diffPath").name("file-0.json"));
    assertThat("Should be 6 new entries", newPaths.size(), equalTo(6));
    Set<String> updatedPaths = FileMetadataUtils.findNewDirectoryPaths(testEntries, pathMap);
    assertThat(
        "Should contain 6 unique directory paths",
        updatedPaths,
        containsInAnyOrder(
            "/_dr_/test/path-0",
            "/_dr_/test/path-1",
            "/_dr_/test/path-2",
            "/_dr_/test/path-3",
            "/_dr_/test/path-4",
            "/_dr_/test/diffPath"));
    assertThat(
        "With LRU size = 2, Path map would contain the last unique path added",
        pathMap,
        hasKey("/_dr_/test/diffPath"));
    assertThat(
        "With LRU size = 2, Path map would contain the common path between directories",
        pathMap,
        hasKey("/_dr_/test"));
  }

  @Test
  public void testShuffledNewPaths() {
    List<FireStoreDirectoryEntry> testEntries = initTestEntries(5);
    Collections.shuffle(testEntries);
    // LRU has enough space for all entries
    LRUMap<String, Boolean> pathMap = new LRUMap<>(2);

    Set<String> newPaths = FileMetadataUtils.findNewDirectoryPaths(testEntries, pathMap);
    // We can't test for specific items in cache since they're shuffled, but we can check for
    // the right number of new, unique paths
    assertThat("Should be 6 new entries", newPaths.size(), equalTo(6));
  }

  private List<FireStoreDirectoryEntry> initTestEntries(int numDirectories) {
    List<FireStoreDirectoryEntry> testEntries = new ArrayList<>();
    // Add two files per different directory path
    for (int i = 0; i < numDirectories; i++) {
      String path = String.format("/test/path-%s", i);
      testEntries.add(new FireStoreDirectoryEntry().path(path).name("file-0.json"));
      testEntries.add(new FireStoreDirectoryEntry().path(path).name("file-1.json"));
      testEntries.add(new FireStoreDirectoryEntry().path(path).name("file-2.json"));
      testEntries.add(new FireStoreDirectoryEntry().path(path).name("file-3.json"));
    }

    return testEntries;
  }
}

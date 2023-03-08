package bio.terra.service.filedata;

import static bio.terra.service.filedata.FileMetadataUtils.ROOT_DIR_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThrows;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.filedata.FileMetadataUtils.Md5ValidationResult;
import bio.terra.service.filedata.FileMetadataUtils.Md5ValidationResult.Md5Type;
import bio.terra.service.filedata.exception.InvalidFileChecksumException;
import bio.terra.service.filedata.google.firestore.FireStoreDirectoryEntry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections4.map.LRUMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@SuppressFBWarnings(
    value = "DMI",
    justification =
        "This fails with not allowing absolute paths but they're not file paths in our case")
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
    // LRU does NOT have enough space for all entries
    LRUMap<String, Boolean> pathMap = new LRUMap<>(2);

    Set<String> newPaths = FileMetadataUtils.findNewDirectoryPaths(testEntries, pathMap);
    // "newPaths" returns all of the new entries, but the pathMap only contains last 2 entries
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

    // Make same call - with one added path. All but the common path was kicked out the cache
    // b/c the cache size is so small, so we'll see 6 unique entries.
    // "/_dr_/test" still in cache
    // "/_dr_/test/path-4" was kicked out of cache, so when we check that entry, we don't know that
    // that it's an existing entry and thus returned as a new entries to cache
    // "/_dr_/test/diffPath" added to cache b/c last file encountered in list
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
  public void testUniqueNewPaths() {
    List<FireStoreDirectoryEntry> testEntries = initTestEntries(5);

    // Checking for unique new paths requires two constraints on the findNewDirectoryPaths method
    // Would this happen in real life?
    // It's a reasonable scenario that all files with the same
    // base path may not be loaded right next to each other in a file ingest
    // 1 - Cache size smaller than the number of unique paths
    LRUMap<String, Boolean> pathMap = new LRUMap<>(2);
    // 2 - Shuffling entries so that files with same base path are not sequential
    // When they're sequential, the first instance will add them to the cache
    // And then automatically won't get added as a new path
    Collections.shuffle(testEntries);

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
  }

  @Test
  public void testValidateFileMd5ForIngest() {
    assertThat(
        "matching md5s pass",
        FileMetadataUtils.validateFileMd5ForIngest("foo", "foo", "gs://bucket/path.txt"),
        equalTo(new Md5ValidationResult("foo", Md5Type.CLOUD_PROVIDED)));

    assertThat(
        "user specified md5 returns if no cloud md5 is present",
        FileMetadataUtils.validateFileMd5ForIngest("foo", null, "gs://bucket/path.txt"),
        equalTo(new Md5ValidationResult("foo", Md5Type.USER_PROVIDED)));

    assertThat(
        "cloud md5 returns if no user specified md5 is present",
        FileMetadataUtils.validateFileMd5ForIngest(null, "foo", "gs://bucket/path.txt"),
        equalTo(new Md5ValidationResult("foo", Md5Type.CLOUD_PROVIDED)));

    assertThat(
        "null returned if both inputs are null",
        FileMetadataUtils.validateFileMd5ForIngest(null, null, "gs://bucket/path.txt"),
        equalTo(new Md5ValidationResult(null, Md5Type.NEITHER)));

    assertThat(
        "empty string returned if both inputs are empty strings",
        FileMetadataUtils.validateFileMd5ForIngest("", "", "gs://bucket/path.txt"),
        equalTo(new Md5ValidationResult("", Md5Type.NEITHER)));

    TestUtils.assertError(
        InvalidFileChecksumException.class,
        "Checksums do not match for file gs://bucket/path.txt",
        () -> FileMetadataUtils.validateFileMd5ForIngest("foo", "bar", "gs://bucket/path.txt"));
  }

  @Test
  public void extractDirectoryPathsTest() {
    assertThat(FileMetadataUtils.extractDirectoryPaths("/foo.txt"), equalTo(List.of("/")));

    assertThat(FileMetadataUtils.extractDirectoryPaths("/"), equalTo(List.of("/")));

    assertThat(
        FileMetadataUtils.extractDirectoryPaths("/foo/bar/baz.txt"),
        equalTo(List.of("/", "/foo", "/foo/bar")));

    assertThrows(IllegalArgumentException.class, () -> FileMetadataUtils.extractDirectoryPaths(""));
  }

  private List<FireStoreDirectoryEntry> initTestEntries(int numDirectories) {
    List<FireStoreDirectoryEntry> testEntries = new ArrayList<>();
    // Add 4 files per different directory path
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

package bio.terra.service.filedata;

import static bio.terra.service.filedata.FileMetadataUtils.ROOT_DIR_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
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
}

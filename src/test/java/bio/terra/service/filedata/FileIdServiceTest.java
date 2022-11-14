package bio.terra.service.filedata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.Dataset;
import java.util.List;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Category(Unit.class)
public class FileIdServiceTest {

  FileIdService service = new FileIdService();

  @Mock Dataset dataset;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testPredictableUUIDEquals() {
    when(dataset.isPredictableFileIds()).thenReturn(true);
    FSItem fsItem1 = new FSFile().path("/foo/bar").size(123L).checksumMd5("foo");

    FSItem fsItem2 = new FSFile().path("/foo/bar").size(123L).checksumMd5("foo");
    assertEquals(
        "IDs match",
        service.calculateFileId(dataset, fsItem1),
        service.calculateFileId(dataset, fsItem2));
  }

  @Test
  public void testPredictableUUIDNotEqualsWhenRandom() {
    when(dataset.isPredictableFileIds()).thenReturn(false);
    FSItem fsItem1 = new FSFile().path("/foo/bar").size(123L).checksumMd5("foo");

    FSItem fsItem2 = new FSFile().path("/foo/bar").size(123L).checksumMd5("foo");
    assertNotEquals(
        "IDs don't match when using random ID mode",
        service.calculateFileId(dataset, fsItem1),
        service.calculateFileId(dataset, fsItem2));
  }

  @Test
  public void testPredictableUUIDNotEquals() {
    when(dataset.isPredictableFileIds()).thenReturn(true);
    record TestCase(FSItem fsItem, FSFileInfo fsFileInfo) {}
    List<TestCase> testCases =
        Stream.of("/foo/bar/file1.txt", "/foo/bar/file2.txt")
            .flatMap(
                p ->
                    Stream.of(123L, 456L)
                        .flatMap(
                            s ->
                                Stream.of("foo", "bar")
                                    .map(
                                        c ->
                                            new TestCase(
                                                new FSFile().path(p).size(s).checksumMd5(c),
                                                new FSFileInfo().size(s).checksumMd5(c)))))
            .toList();

    for (int i = 0; i < testCases.size(); i++) {
      TestCase tc1 = testCases.get(i);
      TestCase tc2 = testCases.get((i + 1) % testCases.size());
      assertNotEquals(
          "IDs don't collide between:\n%s\nand\n%s".formatted(tc1.fsItem, tc2.fsItem),
          service.calculateFileId(dataset, tc1.fsItem),
          service.calculateFileId(dataset, tc2.fsItem));
    }
  }
}

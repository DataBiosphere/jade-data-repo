package bio.terra.service.filedata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.when;

import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.Dataset;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class FileIdServiceTest {

  FileIdService service = new FileIdService();

  @Mock Dataset dataset;

  @Test
  void testPredictableUUIDEquals() {
    when(dataset.hasPredictableFileIds()).thenReturn(true);
    FSItem fsItem1 = new FSFile().path("/foo/bar").size(123L).checksumMd5("foo");

    FSItem fsItem2 = new FSFile().path("/foo/bar").size(123L).checksumMd5("foo");
    assertEquals(
        service.calculateFileId(dataset, fsItem1),
        service.calculateFileId(dataset, fsItem2),
        "IDs match");
  }

  @Test
  void testPredictableUUIDWithMissingFieldsFails() {
    TestUtils.assertError(
        NullPointerException.class,
        "A target path is required to create a file id",
        () -> service.calculateFileId(true, new FSFile().path(null).checksumMd5("md5").size(1L)));
    TestUtils.assertError(
        NullPointerException.class,
        "An MD5 checksum is required to create a file id",
        () -> service.calculateFileId(true, new FSFile().path("/p").checksumMd5(null).size(1L)));
    TestUtils.assertError(
        NullPointerException.class,
        "A size is required to create a file id",
        () -> service.calculateFileId(true, new FSFile().path("/p").checksumMd5("md5").size(null)));
  }

  @Test
  void testPredictableUUIDNotEqualsWhenRandom() {
    when(dataset.hasPredictableFileIds()).thenReturn(false);
    FSItem fsItem1 = new FSFile().path("/foo/bar").size(123L).checksumMd5("foo");

    FSItem fsItem2 = new FSFile().path("/foo/bar").size(123L).checksumMd5("foo");
    assertNotEquals(
        service.calculateFileId(dataset, fsItem1),
        service.calculateFileId(dataset, fsItem2),
        "IDs don't match when using random ID mode");
  }

  record TestCase(FSItem fsItem, FSFileInfo fsFileInfo) {}

  @Test
  void testPredictableUUIDNotEquals() {
    when(dataset.hasPredictableFileIds()).thenReturn(true);
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
          service.calculateFileId(dataset, tc1.fsItem),
          service.calculateFileId(dataset, tc2.fsItem),
          () -> "IDs don't collide between:\n%s\nand\n%s".formatted(tc1.fsItem, tc2.fsItem));
    }
  }
}

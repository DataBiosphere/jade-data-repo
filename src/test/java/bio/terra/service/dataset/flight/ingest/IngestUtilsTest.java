package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.exception.InvalidUriException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class IngestUtilsTest {

    @Test
    public void testParseValidSingleFile() {
        IngestUtils.GsUrlParts parsed = IngestUtils.parseBlobUri("gs://some-bucket/some/file.json");
        assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
        assertThat("path is extracted", parsed.getPath(), equalTo("some/file.json"));
        assertThat("not a wildcard", parsed.getIsWildcard(), equalTo(false));
    }

    @Test
    public void testParseValidPatternAtEnd() {
        IngestUtils.GsUrlParts parsed = IngestUtils.parseBlobUri("gs://some-bucket/some/prefix*");
        assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
        assertThat("path is extracted", parsed.getPath(), equalTo("some/prefix*"));
        assertThat("not a wildcard", parsed.getIsWildcard(), equalTo(true));
    }

    @Test
    public void testParseValidPatternInMiddle() {
        IngestUtils.GsUrlParts parsed = IngestUtils.parseBlobUri("gs://some-bucket/some*pattern");
        assertThat("bucket is extracted", parsed.getBucket(), equalTo("some-bucket"));
        assertThat("path is extracted", parsed.getPath(), equalTo("some*pattern"));
        assertThat("not a wildcard", parsed.getIsWildcard(), equalTo(true));
    }

    @Test(expected = InvalidUriException.class)
    public void testNotAGsUri() {
        IngestUtils.parseBlobUri("https://foo.com/bar");
    }

    @Test(expected = InvalidUriException.class)
    public void testInvalidBucketWildcard() {
        IngestUtils.parseBlobUri("gs://some-bucket-*/some/file/path");
    }

    @Test(expected = InvalidUriException.class)
    public void testInvalidMultiWildcard() {
        IngestUtils.parseBlobUri("gs://some-bucket/some/prefix*/some*pattern");
    }
}

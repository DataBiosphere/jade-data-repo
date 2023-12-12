package bio.terra.service.common.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.azure.storage.blob.BlobUrlParts;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag("bio.terra.common.category.Unit")
class AzureUriUtilsTest {

  private static Stream<Arguments> testEncodedUrls() {
    return Stream.of(
        arguments(
            BlobUrlParts.parse("https://foo.blob.core.windows.net/container/blobname.txt"),
            "https://foo.blob.core.windows.net/container/blobname.txt"),
        arguments(
            BlobUrlParts.parse("https://foo.blob.core.windows.net/container/blob/name.txt"),
            "https://foo.blob.core.windows.net/container/blob/name.txt"),
        arguments(
            BlobUrlParts.parse("https://foo.blob.core.windows.net/container/blobname.txt?sp=r"),
            "https://foo.blob.core.windows.net/container/blobname.txt?sp=r"));
  }

  @ParameterizedTest
  @MethodSource
  void testEncodedUrls(BlobUrlParts urlParts, String expectedUrl) {
    assertThat(
        "url looks as expected",
        AzureUriUtils.getUriFromBlobUrlParts(urlParts),
        equalTo(expectedUrl));
  }
}

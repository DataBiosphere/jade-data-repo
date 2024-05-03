package bio.terra.service.filedata.azure.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class BlobContainerCopierBuilderTest {

  @Mock private BlobContainerClientFactory sourceFactory;
  @Mock private BlobContainerClientFactory destinationFactory;
  @Mock private List<BlobCopySourceDestinationPair> pairs;

  private BlobContainerCopier copier;

  @Test
  void testBuildCopierUsingSourceAndDestinationFactories_CopierIsBuilt() {

    copier =
        new BlobContainerCopierBuilder()
            .sourceClientFactory(sourceFactory)
            .destinationClientFactory(destinationFactory)
            .build();

    assertThat(
        copier,
        allOf(
            hasProperty("sourceClientFactory", equalTo(sourceFactory)),
            hasProperty("destinationClientFactory", equalTo(destinationFactory))));
  }

  @Test
  void testBuildCopierUsingSourceAndDestinationFactoriesWithPairs_CopierIsBuilt() {

    copier =
        new BlobContainerCopierBuilder()
            .sourceClientFactory(sourceFactory)
            .destinationClientFactory(destinationFactory)
            .sourceDestinationPairs(pairs)
            .build();

    assertThat(
        copier,
        allOf(
            hasProperty("sourceClientFactory", equalTo(sourceFactory)),
            hasProperty("destinationClientFactory", equalTo(destinationFactory)),
            hasProperty("sourceDestinationPairs", equalTo(pairs))));
  }

  @Test
  void testBuildCopierUsingSourceBlobUrlAndDestinationFactories_CopierIsBuilt() {

    String sourceBlobUrl = "https://mytest.blob.core.windows.net/mytest/test?sp=rl";
    String destinationBlobName = "destBlobName";

    copier =
        new BlobContainerCopierBuilder()
            .sourceBlobUrl(sourceBlobUrl)
            .destinationBlobName(destinationBlobName)
            .destinationClientFactory(destinationFactory)
            .build();

    assertThat(
        copier,
        allOf(
            hasProperty("sourceBlobUrl", equalTo(sourceBlobUrl)),
            hasProperty("destinationBlobName", equalTo(destinationBlobName)),
            hasProperty("destinationClientFactory", equalTo(destinationFactory))));
  }

  @Test
  void testBuildCopierUsingSourceGCSBlobUrlAndDestinationFactories_CopierIsBuilt() {

    String sourceBlobUrl = "gs://mybucket/my.blob.txt";
    String destinationBlobName = "destBlobName";

    copier =
        new BlobContainerCopierBuilder()
            .sourceBlobUrl(sourceBlobUrl)
            .destinationBlobName(destinationBlobName)
            .destinationClientFactory(destinationFactory)
            .build();

    assertThat(
        copier,
        allOf(
            hasProperty("sourceBlobUrl", equalTo(sourceBlobUrl)),
            hasProperty("destinationBlobName", equalTo(destinationBlobName)),
            hasProperty("destinationClientFactory", equalTo(destinationFactory))));
  }

  @Test
  void testSourceFactoryIsMissing_ThrowsIllegalArgumentException() {
    BlobContainerCopierBuilder blobContainerCopierBuilder =
        new BlobContainerCopierBuilder().destinationClientFactory(destinationFactory);
    assertThrows(IllegalArgumentException.class, blobContainerCopierBuilder::build);
  }
}

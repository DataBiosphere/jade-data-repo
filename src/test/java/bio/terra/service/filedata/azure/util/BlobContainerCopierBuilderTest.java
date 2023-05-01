package bio.terra.service.filedata.azure.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;

import bio.terra.common.category.Unit;
import java.util.List;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class BlobContainerCopierBuilderTest {

  @Mock private BlobContainerClientFactory sourceFactory;
  @Mock private BlobContainerClientFactory destinationFactory;
  @Mock private List<BlobCopySourceDestinationPair> pairs;

  private BlobContainerCopier copier;

  @Test
  public void testBuildCopierUsingSourceAndDestinationFactories_CopierIsBuilt() {

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
  public void testBuildCopierUsingSourceAndDestinationFactoriesWithPairs_CopierIsBuilt() {

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
  public void testBuildCopierUsingSourceBlobUrlAndDestinationFactories_CopierIsBuilt() {

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
  public void testBuildCopierUsingSourceGCSBlobUrlAndDestinationFactories_CopierIsBuilt() {

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

  @Test(expected = IllegalArgumentException.class)
  public void testSourceFactoryIsMissing_ThrowsIllegalArgumentException() {
    copier = new BlobContainerCopierBuilder().destinationClientFactory(destinationFactory).build();
  }
}

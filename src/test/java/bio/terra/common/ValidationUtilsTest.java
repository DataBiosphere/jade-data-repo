package bio.terra.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import bio.terra.common.category.Unit;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class ValidationUtilsTest {

  @Test
  void testEmailFormats() throws Exception {
    assertThat(ValidationUtils.isValidEmail("john@somewhere.com")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john.foo@somewhere.com")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john.foo+label@somewhere.com")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john@192.168.1.10")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john+label@192.168.1.10")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john.foo@someserver")).isTrue();
    assertThat(ValidationUtils.isValidEmail("JOHN.FOO@somewhere.com")).isTrue();
    assertThat(ValidationUtils.isValidEmail("@someserver")).isFalse();
    assertThat(ValidationUtils.isValidEmail("@someserver.com")).isFalse();
    assertThat(ValidationUtils.isValidEmail("john@.")).isFalse();
    assertThat(ValidationUtils.isValidEmail(".@somewhere.com")).isFalse();
  }

  @Test
  void testHasDuplicates() {
    assertThat(ValidationUtils.hasDuplicates(Arrays.asList("a", "a", "b", "c"))).isTrue();
    assertThat(ValidationUtils.hasDuplicates(Arrays.asList(1, 1, 2, 3))).isTrue();
    assertThat(ValidationUtils.hasDuplicates(Arrays.asList("a", "b", "c"))).isFalse();
    assertThat(ValidationUtils.hasDuplicates(Arrays.asList(1, 2, 3))).isFalse();
    assertThat(ValidationUtils.hasDuplicates(Collections.emptyList())).isFalse();
    assertThat(ValidationUtils.hasDuplicates(Collections.singletonList("a"))).isFalse();
  }

  @Test
  void testDescriptionFormats() throws Exception {
    assertThat(ValidationUtils.isValidDescription("somedescription")).isTrue();
    assertThat(ValidationUtils.isValidDescription(StringUtils.repeat("X", 5_000))).isFalse();
  }

  @Test
  void testPathFormats() throws Exception {
    assertThat(ValidationUtils.isValidPath("/some_path")).isTrue();
    assertThat(ValidationUtils.isValidPath("some_path")).isFalse();
  }

  @Test
  void testUuidFormats() {
    assertThat(ValidationUtils.isValidUuid("2c297e7c-b303-4243-af6a-76cd9d3b0ca8")).isTrue();
    assertThat(ValidationUtils.convertToUuid("2c297e7c-b303-4243-af6a-76cd9d3b0ca8")).isPresent();
    assertThat(ValidationUtils.isValidUuid("not a uuid")).isFalse();
    assertThat(ValidationUtils.convertToUuid("not a uuid")).isEmpty();
  }

  @Test
  void testIsValidBucketName() {
    assertThat(ValidationUtils.isValidBucketName("bucket")).isTrue();
    assertThat(ValidationUtils.isValidBucketName("bucket-1")).isTrue();
    assertThat(ValidationUtils.isValidBucketName("1-bucket")).isTrue();
    assertThat(
            ValidationUtils.isValidBucketName(
                "bucket-123456789-123456789-123456789-123456789-123456789"))
        .isTrue();
    assertThat(ValidationUtils.isValidBucketName("bucket-")).isFalse();
    assertThat(ValidationUtils.isValidBucketName("-bucket")).isFalse();
    assertThat(
            ValidationUtils.isValidBucketName(
                "bucket-123456789-123456789-123456789-123456789-123456789-123456789-123456789"))
        .isFalse();
  }

  @Test
  void testConvertToBucketName() {
    assertThat(ValidationUtils.convertToBucketName("bucket")).isPresent();
    assertThat(ValidationUtils.convertToBucketName("bucket-1")).isPresent();
    assertThat(ValidationUtils.convertToBucketName("1-bucket")).isPresent();
    assertThat(
            ValidationUtils.convertToBucketName(
                "bucket-123456789-123456789-123456789-123456789-123456789"))
        .isPresent();
    assertThat(ValidationUtils.convertToBucketName("bucket-")).isEmpty();
    assertThat(ValidationUtils.convertToBucketName("-bucket")).isEmpty();
    assertThat(
            ValidationUtils.convertToBucketName(
                "bucket-123456789-123456789-123456789-123456789-123456789-123456789-123456789"))
        .isEmpty();
  }

  @Test
  void testValidationOfEmptyBlankAndNullStrings() {

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ValidationUtils.requireNotBlank("", "empty arg"));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ValidationUtils.requireNotBlank(null, "null arg"));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ValidationUtils.requireNotBlank("  ", "param"));
  }

  @Test
  void testValidationOfValidInputString() {
    // no exception is thrown
    ValidationUtils.requireNotBlank("abc", "error msg");
  }
}

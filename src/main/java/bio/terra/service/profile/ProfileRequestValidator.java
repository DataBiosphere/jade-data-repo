package bio.terra.service.profile;

import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import jakarta.validation.constraints.NotNull;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Component
public class ProfileRequestValidator implements Validator {

  // This is specific to Google's billing account id representation. This will likely need to be
  // split into cloud
  // specific subclasses tagged with profiles.
  private static final String VALID_BILLING_ACCOUNT_ID_REGEX =
      "[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}";

  @Override
  public boolean supports(Class<?> clazz) {
    return true;
  }

  @Override
  public void validate(@NotNull Object target, Errors errors) {
    if (target != null && target instanceof BillingProfileRequestModel) {
      BillingProfileRequestModel billingProfileRequestModel = (BillingProfileRequestModel) target;
      isValidCloudPlatform(billingProfileRequestModel, errors);
    }
  }

  public static boolean isValidAccountId(String billingAccountId) {
    return Pattern.matches(VALID_BILLING_ACCOUNT_ID_REGEX, billingAccountId);
  }

  public static void isValidCloudPlatform(
      BillingProfileRequestModel billingProfileRequestModel, Errors errors) {
    String billingAccountId = billingProfileRequestModel.getBillingAccountId();
    if (billingProfileRequestModel.getCloudPlatform() == CloudPlatform.AZURE) {
      String errorCode = "For Azure, a valid UUID `%s` must be provided";
      if (billingProfileRequestModel.getTenantId() == null) {
        errors.rejectValue("tenantId", String.format(errorCode, "tenantId"));
      }
      if (billingProfileRequestModel.getSubscriptionId() == null) {
        errors.rejectValue("subscriptionId", String.format(errorCode, "subscriptionId"));
      }
      if (billingProfileRequestModel.getResourceGroupName() == null
          || billingProfileRequestModel.getResourceGroupName().isEmpty()) {
        errors.rejectValue(
            "resourceGroupName", "For Azure, a non-empty resourceGroupName must be provided");
      }
      if (billingAccountId != null && !billingAccountId.isEmpty()) {
        errors.rejectValue(
            "billingAccountId", "For Azure, the Google billing account id must not be provided.");
      }
    } else {
      // GCP is the default cloud platform, so there should be no Azure info from here on.
      String errorCode = "For GCP, no Azure information should be provided";
      if (billingProfileRequestModel.getTenantId() != null) {
        errors.rejectValue("tenantId", errorCode);
      }
      if (billingProfileRequestModel.getSubscriptionId() != null) {
        errors.rejectValue("subscriptionId", errorCode);
      }
      if (billingProfileRequestModel.getResourceGroupName() != null) {
        errors.rejectValue("resourceGroupName", errorCode);
      }
      if (billingAccountId == null || !isValidAccountId(billingAccountId)) {
        errors.rejectValue(
            "billingAccountId",
            "The id must be 3 sets of 6 capitalized alphanumeric characters separated by dashes");
      }
    }
  }
}

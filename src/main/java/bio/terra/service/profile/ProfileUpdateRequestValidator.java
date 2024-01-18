package bio.terra.service.profile;

import bio.terra.model.BillingProfileUpdateModel;
import jakarta.validation.constraints.NotNull;
import org.springframework.stereotype.Component;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Component
public class ProfileUpdateRequestValidator implements Validator {

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
    if (target != null && target instanceof BillingProfileUpdateModel) {
      BillingProfileUpdateModel billingProfileUpdateModel = (BillingProfileUpdateModel) target;
      String billingAccountId = billingProfileUpdateModel.getBillingAccountId();
      if (billingAccountId == null || !ProfileRequestValidator.isValidAccountId(billingAccountId)) {
        errors.rejectValue(
            "billingAccountId",
            "The id must be 3 sets of 6 capitalized alphanumeric characters separated by dashes");
      }
      if (billingProfileUpdateModel.getId() == null) {
        errors.rejectValue("id", "The billing profile id must be specified");
      }
    }
  }
}

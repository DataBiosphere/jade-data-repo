package bio.terra.service.dataset;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import bio.terra.model.ErrorModel;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.hamcrest.Matcher;

public class ValidatorTestUtils {

  public static void checkValidationErrorModel(ErrorModel errorModel, String[] messageCodes) {
    checkValidationErrorModel(errorModel, Arrays.asList(messageCodes));
  }

  public static void checkValidationErrorModel(ErrorModel errorModel, List<String> messageCodes) {
    List<String> details = errorModel.getErrorDetail();
    assertThat(
        "Main message is right",
        errorModel.getMessage(),
        containsString("Validation errors - see error details"));
    /*
     * The global exception handler logs in this format:
     *
     * <fieldName>: '<messageCode>' (<defaultMessage>)
     *
     * We check to see if the code is wrapped in quotes to prevent matching on substrings.
     */
    List<Matcher<? super String>> expectedMatches =
        messageCodes.stream()
            .map(code -> containsString("'" + code + "'"))
            .collect(Collectors.toList());
    assertThat("Detail codes are right", details, containsInAnyOrder(expectedMatches));
  }
}

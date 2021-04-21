package bio.terra.service.iam.sam;


import bio.terra.common.category.Unit;
import org.broadinstitute.dsde.workbench.client.sam.model.ErrorReport;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;

@Category(Unit.class)
public class SamIamTest {

    @Test
    public void testExtractErrorMessageSimple() {
        ErrorReport errorReport = new ErrorReport()
            .message("FOO")
            .source("sam");

        assertThat(SamIam.extractErrorMessage(errorReport)).isEqualTo("FOO");
    }

    @Test
    public void testExtractErrorMessageSimpleNested() {
        ErrorReport errorReport = new ErrorReport()
            .message("FOO")
            .source("sam")
            .addCausesItem(new ErrorReport()
                .message("BAR")
                .source("sam"));

        assertThat(SamIam.extractErrorMessage(errorReport)).isEqualTo("FOO: BAR");
    }

    @Test
    public void testExtractErrorMessageDeepNested() {
        ErrorReport errorReport = new ErrorReport()
            .message("FOO")
            .source("sam")
            .addCausesItem(new ErrorReport()
                .message("BAR")
                .source("sam")
                .addCausesItem(
                    new ErrorReport()
                        .message("BAZ1")
                        .source("sam")
                )
                .addCausesItem(
                    new ErrorReport()
                        .message("BAZ2")
                        .source("sam")
                        .addCausesItem(new ErrorReport()
                            .message("QUX")
                            .source("sam"))
                )
            );

        assertThat(SamIam.extractErrorMessage(errorReport)).isEqualTo("FOO: BAR: (BAZ1, BAZ2: QUX)");
    }
}

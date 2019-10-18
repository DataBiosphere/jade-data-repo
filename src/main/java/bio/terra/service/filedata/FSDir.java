package bio.terra.service.filedata;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;

public class FSDir extends FSItem {
    private List<FSItem> contents;
    private boolean enumerated;

    public FSDir() {
    }

    public boolean isEnumerated() {
        return enumerated;
    }

    public List<FSItem> getContents() {
        return contents;
    }


    public FSDir contents(List<FSItem> contents) {
        this.enumerated = true;
        this.contents = contents;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        FSDir fsDir = (FSDir) o;

        return new EqualsBuilder()
            .appendSuper(super.equals(o))
            .append(contents, fsDir.contents)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .appendSuper(super.hashCode())
            .append(contents)
            .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
            .append("contents", contents)
            .toString();
    }
}

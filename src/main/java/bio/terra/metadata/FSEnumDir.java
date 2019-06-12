package bio.terra.metadata;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;

public class FSEnumDir extends FSObjectBase {
    private List<FSObjectBase> contents;

    public FSEnumDir() {
    }

    public List<FSObjectBase> getContents() {
        return contents;
    }

    public FSEnumDir contents(List<FSObjectBase> contents) {
        this.contents = contents;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        FSEnumDir fsEnumDir = (FSEnumDir) o;

        return new EqualsBuilder()
            .appendSuper(super.equals(o))
            .append(contents, fsEnumDir.contents)
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

package bio.terra.service.dataset;

import bio.terra.common.Column;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class AssetTable {
    private DatasetTable datasetTable;
    private List<AssetColumn> columns = new ArrayList<>();

    public DatasetTable getTable() {
        return datasetTable;
    }

    public AssetTable datasetTable(DatasetTable datasetTable) {
        this.datasetTable = datasetTable;
        return this;
    }

    public Collection<AssetColumn> getColumns() {
        return columns;
    }

    public AssetTable columns(List<AssetColumn> columns) {
        this.columns = columns;
        return this;
    }

    public Optional<Column> getDatasetColumnByName(String name) {
        for (AssetColumn assetColumn : getColumns()) {
            Column datasetColumn = assetColumn.getDatasetColumn();
            if (StringUtils.equals(name, datasetColumn.getName())) {
                return Optional.of(datasetColumn);
            }
        }
        return Optional.empty();
    }

}

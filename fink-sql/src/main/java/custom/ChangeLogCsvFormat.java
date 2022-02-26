package custom;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

public class ChangeLogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    private final String columnDelimiter;

    public ChangeLogCsvFormat(String columnDelimiter) {
        this.columnDelimiter = columnDelimiter;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType physicalDataType) {
        final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(physicalDataType);
        DynamicTableSource.DataStructureConverter converter = context.createDataStructureConverter(physicalDataType);

        List<LogicalType> logicalTypes = physicalDataType.getLogicalType().getChildren();

        return new ChangelogCsvDeserializer(logicalTypes,converter,producedTypeInfo,columnDelimiter);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE).build();
    }
}

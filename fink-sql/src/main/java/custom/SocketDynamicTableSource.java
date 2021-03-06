package custom;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class SocketDynamicTableSource implements ScanTableSource {
    private final String hostname;
    private final int port;
    private final byte byteDelimiter;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType produceDataType;

    public SocketDynamicTableSource(String hostname, int port, byte byteDelimiter, DecodingFormat<DeserializationSchema<RowData>> decodingFormat, DataType produceDataType) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
        this.decodingFormat = decodingFormat;
        this.produceDataType = produceDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(runtimeProviderContext, produceDataType);

        final SourceFunction<RowData> sourceFunction = new SocketSourceFunction(
                hostname,
                port,
                byteDelimiter,
                deserializer);

        return SourceFunctionProvider.of(sourceFunction,false);
    }

    @Override
    public DynamicTableSource copy() {
        return new SocketDynamicTableSource(hostname,port,byteDelimiter,decodingFormat,produceDataType);
    }

    @Override
    public String asSummaryString() {
        return "socket table source";
    }
}

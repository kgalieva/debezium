/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.pipeline.source.snapshot.incremental;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

public class IncrementalSnapshotSchema {
    private final int columnCount;
    private final ColumnSchema[] columnSchemas;

    public IncrementalSnapshotSchema(ResultSetMetaData metaData) throws SQLException {
        this.columnCount = metaData.getColumnCount();
        columnSchemas = new ColumnSchema[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            columnSchemas[i - 1] = new ColumnSchema(metaData.getColumnName(i),
                    metaData.getColumnType(i),
                    metaData.isNullable(i),
                    metaData.getPrecision(i),
                    metaData.getScale(i));
        }
    }

    public class ColumnSchema {
        String columnName;
        int columnType;
        int nullable;
        int precision;
        int scale;

        public ColumnSchema(String columnName, int columnType, int nullable, int precision, int scale) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.nullable = nullable;
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ColumnSchema that = (ColumnSchema) o;
            return columnType == that.columnType && nullable == that.nullable && precision == that.precision && scale == that.scale && columnName.equals(that.columnName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(columnName, columnType, nullable, precision, scale);
        }

        @Override
        public String toString() {
            return "ColumnSchema{" +
                    "columnName='" + columnName + '\'' +
                    ", columnType=" + columnType +
                    ", nullable=" + nullable +
                    ", precision=" + precision +
                    ", scale=" + scale +
                    '}';
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IncrementalSnapshotSchema schema = (IncrementalSnapshotSchema) o;
        return columnCount == schema.columnCount && Arrays.equals(columnSchemas, schema.columnSchemas);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(columnCount);
        result = 31 * result + Arrays.hashCode(columnSchemas);
        return result;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "columnCount=" + columnCount +
                ", columnSchemas=" + Arrays.toString(columnSchemas) +
                '}';
    }
}

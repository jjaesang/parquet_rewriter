package com.github.henry.convert;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Set;

public class NullValueConveter implements ValueConverter {

    private MessageType schema;
    private SimpleGroupFactory rowGroupFactory;
    private Set<String> targetColNameSet;
    private String keyColName;

    public NullValueConveter(MessageType schema, Set<String> targetColNameSet, String keyColName) {
        this.schema = schema;
        this.rowGroupFactory = new SimpleGroupFactory(schema);
        this.targetColNameSet = targetColNameSet;
        this.keyColName = keyColName;
    }

    @Override
    public Group convertGroup(Group group) {

        if (isNotConvertRow(group)) {
            return group;
        }

        Group targetGroup = rowGroupFactory.newGroup();
        int fieldCount = schema.getFieldCount();
        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            if (isConvertNullColumn(fieldIndex)) {
                continue;
            }
            convertColumnValue(targetGroup, fieldIndex, group);
        }

        return targetGroup;
    }

    private boolean isConvertRow(Group group) {
        int keyColIndex = schema.getFieldIndex(keyColName);
        return targetColNameSet.contains(group.getValueToString(keyColIndex, 0));
    }

    private boolean isNotConvertRow(Group group) {
        return !isConvertRow(group);
    }

    private boolean isConvertNullColumn(int fieldIndex) {
        return targetColNameSet.contains(schema.getFieldName(fieldIndex));
    }

    private boolean isNotConvertNullColumn(int fieldIndex) {
        return !isConvertNullColumn(fieldIndex);
    }

    private void convertColumnValue(Group targetGroup, int index, Group sourceGroup) {
        GroupType type = sourceGroup.getType();
        PrimitiveType.PrimitiveTypeName columnType = type.getType(index).asPrimitiveType()
                .getPrimitiveTypeName();
        String fieldName = type.getFieldName(index);

        int fieldRepetitionCount = sourceGroup.getFieldRepetitionCount(index);
        for (int fieldIndex = 0; fieldIndex < fieldRepetitionCount; fieldIndex++) {
            switch (columnType) {
                case BINARY:
                    targetGroup.add(fieldName, sourceGroup.getBinary(schema.getFieldName(index), fieldIndex));
                    break;
                case BOOLEAN:
                    targetGroup.add(fieldName, sourceGroup.getBoolean(schema.getFieldName(index), fieldIndex));
                    break;
                case DOUBLE:
                    targetGroup.add(fieldName, sourceGroup.getDouble(schema.getFieldName(index), fieldIndex));
                    break;
                case FLOAT:
                    targetGroup.add(fieldName, sourceGroup.getFloat(schema.getFieldName(index), fieldIndex));
                    break;
                case INT32:
                    targetGroup.add(fieldName, sourceGroup.getInteger(schema.getFieldName(index), fieldIndex));
                    break;
                case INT64:
                    targetGroup.add(fieldName, sourceGroup.getLong(schema.getFieldName(index), fieldIndex));
                    break;
                case INT96:
                    targetGroup.add(fieldName, sourceGroup.getInt96(schema.getFieldName(index), fieldIndex));
                    break;
                default:
                    throw new IllegalArgumentException("Not Support Type!");
            }
        }
    }
}

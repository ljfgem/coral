/**
 * Copyright 2019-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.schema.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.Jackson1Utils;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.codehaus.jackson.node.JsonNodeFactory;

import com.linkedin.coral.com.google.common.collect.Lists;


/**
 * This class converts Hive TypeInfo schema to avro schema
 *
 * The main API is convertFieldsTypeInfoToAvroSchema
 */
public class TypeInfoToAvroSchemaConverter {
  private int recordCounter;
  private final String namespace;

  private final boolean mkFieldsOptional;

  // Additional numeric type, similar to other logical type names in AvroSerde
  private static final String SHORT_TYPE_NAME = "short";
  private static final String BYTE_TYPE_NAME = "byte";

  public TypeInfoToAvroSchemaConverter(String namespace, boolean mkFieldsOptional) {
    this.recordCounter = 0;
    this.namespace = namespace;

    this.mkFieldsOptional = mkFieldsOptional;
  }

  Schema convertFieldsTypeInfoToAvroSchema(String recordNamespace, String recordName, List<String> fieldNames,
      List<TypeInfo> fieldTypeInfos) {
    final List<Schema.Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldNames.size(); ++i) {
      final TypeInfo fieldTypeInfo = fieldTypeInfos.get(i);
      String fieldName = fieldNames.get(i);
      fieldName = removePrefix(fieldName);

      // If there's a structType in the schema, we will use "recordNamespace.fieldName" instead of the
      // autogenerated record name. The recordNamespace is composed of its parent's field names recursively.
      // This mimics the logic of spark-avro.
      // We will set the recordName to be capitalized, and the recordNameSpace will be in lower case
      final Schema schema = convertTypeInfoToAvroSchema(fieldTypeInfo, recordNamespace + "." + recordName.toLowerCase(),
          StringUtils.capitalize(fieldName));
      final Schema.Field f = AvroCompatibilityHelper.createSchemaField(fieldName, schema, null, null);
      fields.add(f);
    }

    final Schema recordSchema = Schema.createRecord(recordName, null, namespace + recordNamespace, false);
    recordSchema.setFields(fields);
    return recordSchema;
  }

  Schema convertTypeInfoToAvroSchema(TypeInfo typeInfo, String recordNamespace, String recordName) {
    Schema schema;
    ObjectInspector.Category c = typeInfo.getCategory();

    switch (c) {
      case STRUCT:
        // We don't cache the structType because otherwise it could be possible that a field
        // "lastname" is of type "firstname", where firstname is a compiled class.
        // This will lead to ambiguity.
        schema = parseSchemaFromStruct((StructTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      case LIST:
        schema = parseSchemaFromList((ListTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      case MAP:
        schema = parseSchemaFromMap((MapTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      case PRIMITIVE:
        schema = parseSchemaFromPrimitive((PrimitiveTypeInfo) typeInfo);
        break;
      case UNION:
        schema = parseSchemaFromUnion((UnionTypeInfo) typeInfo, recordNamespace, recordName);
        break;
      default:
        throw new UnsupportedOperationException("Conversion from " + c + " not supported");
    }

    if (mkFieldsOptional) {
      return wrapInNullableUnion(schema);
    }
    return schema;
  }

  private Schema parseSchemaFromUnion(UnionTypeInfo typeInfo, final String recordNamespace, final String recordName) {
    List<TypeInfo> typeInfos = typeInfo.getAllUnionObjectTypeInfos();

    // A union might contain duplicate struct typeinfos because the underlying Avro union has two Record types with
    // different names but the same internal structure. For example, in tracking.CommunicationRequestEvent.specificRequest,
    // PropGenerated and PropExternalCommunication have the same structure. In case of duplicate typeinfos, we generate
    // a new record type for the duplicates.
    List<Schema> schemas = new ArrayList<>();

    for (TypeInfo ti : typeInfos) {
      Schema candidate;
      if (ti instanceof StructTypeInfo) {
        StructTypeInfo sti = (StructTypeInfo) ti;

        // In case we have several structType in the same level,
        // we need to add numbers to the record name to distinguish them from each other.
        final String newRecordName = recordName + recordCounter;
        recordCounter += 1;

        candidate = parseSchemaFromStruct(sti, recordNamespace, newRecordName);
      } else { // not a struct type
        candidate = convertTypeInfoToAvroSchema(ti, recordNamespace, recordName);
      }

      // Remove nullable wrapping from nested schemas before adding
      schemas.add(AvroSerdeUtils.isNullableType(candidate) ? AvroSerdeUtils.getOtherTypeFromNullableType(candidate)
          : candidate);
    }

    return Schema.createUnion(schemas);
  }

  // Previously, Hive use recordType[N] as the recordName for each structType,
  // now the new record name will be in the form of "structNamespace.structName" given the change made earlier.
  private Schema parseSchemaFromStruct(final StructTypeInfo typeInfo, final String recordNamespace,
      final String recordName) {
    final Schema recordSchema = convertFieldsTypeInfoToAvroSchema(recordNamespace, recordName,
        typeInfo.getAllStructFieldNames(), typeInfo.getAllStructFieldTypeInfos());

    return recordSchema;
  }

  private Schema parseSchemaFromList(final ListTypeInfo typeInfo, final String recordNamespace,
      final String recordName) {
    Schema listSchema = convertTypeInfoToAvroSchema(typeInfo.getListElementTypeInfo(), recordNamespace, recordName);
    return Schema.createArray(listSchema);
  }

  private Schema parseSchemaFromMap(final MapTypeInfo typeInfo, final String recordNamespace, final String recordName) {
    final TypeInfo keyTypeInfo = typeInfo.getMapKeyTypeInfo();
    final PrimitiveObjectInspector.PrimitiveCategory pc = ((PrimitiveTypeInfo) keyTypeInfo).getPrimitiveCategory();
    if (pc != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UnsupportedOperationException("Key of Map can only be a String");
    }

    final TypeInfo valueTypeInfo = typeInfo.getMapValueTypeInfo();
    final Schema valueSchema = convertTypeInfoToAvroSchema(valueTypeInfo, recordNamespace, recordName);

    return Schema.createMap(valueSchema);
  }

  private Schema parseSchemaFromPrimitive(PrimitiveTypeInfo primitiveTypeInfo) {
    Schema schema;
    switch (primitiveTypeInfo.getPrimitiveCategory()) {
      case LONG:
        schema = Schema.create(Schema.Type.LONG);
        break;

      case DATE:
        schema = Schema.create(Schema.Type.INT);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, AvroSerDe.DATE_TYPE_NAME);
        break;

      case TIMESTAMP:
        schema = Schema.create(Schema.Type.LONG);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, AvroSerDe.TIMESTAMP_TYPE_NAME);
        break;

      case BINARY:
        schema = Schema.create(Schema.Type.BYTES);
        break;
      case BOOLEAN:
        schema = Schema.create(Schema.Type.BOOLEAN);
        break;

      case DOUBLE:
        schema = Schema.create(Schema.Type.DOUBLE);
        break;

      case DECIMAL:
        DecimalTypeInfo dti = (DecimalTypeInfo) primitiveTypeInfo;
        JsonNodeFactory factory = JsonNodeFactory.instance;
        schema = Schema.create(Schema.Type.BYTES);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, AvroSerDe.DECIMAL_TYPE_NAME);
        AvroCompatibilityHelper.setSchemaPropFromJsonString(schema, AvroSerDe.AVRO_PROP_PRECISION,
            Jackson1Utils.toJsonString(factory.numberNode(dti.getPrecision())), false);
        AvroCompatibilityHelper.setSchemaPropFromJsonString(schema, AvroSerDe.AVRO_PROP_SCALE,
            Jackson1Utils.toJsonString(factory.numberNode(dti.getScale())), false);
        break;

      case FLOAT:
        schema = Schema.create(Schema.Type.FLOAT);
        break;

      case BYTE:
        schema = Schema.create(Schema.Type.INT);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, BYTE_TYPE_NAME);
        break;

      case SHORT:
        schema = Schema.create(Schema.Type.INT);
        schema.addProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE, SHORT_TYPE_NAME);
        break;

      case INT:
        schema = Schema.create(Schema.Type.INT);
        break;

      case CHAR:
      case STRING:
      case VARCHAR:
        schema = Schema.create(Schema.Type.STRING);
        break;

      case VOID:
        schema = Schema.create(Schema.Type.NULL);
        break;

      default:
        throw new UnsupportedOperationException(primitiveTypeInfo + " is not supported.");
    }
    return schema;
  }

  private static Schema wrapInNullableUnion(Schema schema) {
    Schema wrappedSchema = schema;
    switch (schema.getType()) {
      case NULL:
        break;
      case UNION:
        List<Schema> unionSchemas = Lists.newArrayList(Schema.create(Schema.Type.NULL));
        unionSchemas.addAll(schema.getTypes());
        wrappedSchema = Schema.createUnion(unionSchemas);
        break;
      default:
        wrappedSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
        break;
    }
    return wrappedSchema;
  }

  private static String removePrefix(String name) {
    final int idx = name.lastIndexOf('.');
    if (idx > 0) {
      return name.substring(idx + 1);
    }
    return name;
  }
}

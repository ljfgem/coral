/**
 * Copyright 2022-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.transformers;

import java.math.BigDecimal;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.linkedin.coral.common.transformers.OperatorTransformer;


/**
 * Transformer to convert SqlCall from array[i] to array[i+1] to ensure array indexes start at 1.
 */
public class OneBasedArrayIndexTransformer extends OperatorTransformer {
  @Override
  public boolean condition() {
    if (inputSqlNode instanceof SqlBasicCall
        && "ITEM".equalsIgnoreCase(((SqlBasicCall) inputSqlNode).getOperator().getName())) {
      final SqlNode columnNode = ((SqlBasicCall) inputSqlNode).getOperandList().get(0);
      return getRelDataType(columnNode) instanceof ArraySqlType;
    }
    return false;
  }

  @Override
  public SqlNode transform() {
    final SqlNode itemNode = ((SqlBasicCall) inputSqlNode).getOperandList().get(1);
    if (itemNode instanceof SqlNumericLiteral
        && getRelDataType(itemNode).getSqlTypeName().equals(SqlTypeName.INTEGER)) {
      final Integer value = ((SqlNumericLiteral) itemNode).getValueAs(Integer.class);
      ((SqlBasicCall) inputSqlNode).setOperand(1,
          SqlNumericLiteral.createExactNumeric(new BigDecimal(value + 1).toString(), itemNode.getParserPosition()));
    } else {
      final SqlCall oneBasedIndex = SqlStdOperatorTable.PLUS.createCall(itemNode.getParserPosition(), itemNode,
          SqlNumericLiteral.createExactNumeric("1", SqlParserPos.ZERO));
      ((SqlBasicCall) inputSqlNode).setOperand(1, oneBasedIndex);
    }
    return inputSqlNode;
  }
}

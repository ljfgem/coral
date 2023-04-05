/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.trino.rel2trino.transformers;

import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.linkedin.coral.common.functions.FunctionReturnTypes;
import com.linkedin.coral.common.transformers.SqlCallTransformer;

import static org.apache.calcite.sql.type.ReturnTypes.*;
import static org.apache.calcite.sql.type.SqlTypeName.*;


/**
 * This class implements the transformation for the FROM_UNIXTIME function.
 * It transforms the FROM_UNIXTIME function to FORMAT_DATETIME(FROM_UNIXTIME(...), format) to ensure
 * compatibility with Trino.
 *
 * Example:
 *  FROM_UNIXTIME(1609459200) is transformed into
 *  FORMAT_DATETIME(FROM_UNIXTIME(1609459200), 'yyyy-MM-dd HH:mm:ss')
 */
public class FromUnixtimeTransformer extends SqlCallTransformer {

  private static final String FROM_UNIXTIME = "from_unixtime";
  private static final String FORMAT_DATETIME = "format_datetime";

  @Override
  protected boolean condition(SqlCall sqlCall) {
    return sqlCall.getOperator().getName().equalsIgnoreCase(FROM_UNIXTIME);
  }

  @Override
  protected SqlCall transform(SqlCall sqlCall) {
    List<SqlNode> operands = sqlCall.getOperandList();
    SqlOperator fromUnixtime = createSqlOperator(FROM_UNIXTIME, explicit(TIMESTAMP));
    SqlOperator formatDatetime = createSqlOperator(FORMAT_DATETIME, FunctionReturnTypes.STRING);

    if (operands.size() == 1) {
      return formatDatetime.createCall(SqlParserPos.ZERO, fromUnixtime.createCall(SqlParserPos.ZERO, operands.get(0)),
          SqlLiteral.createCharString("yyyy-MM-dd HH:mm:ss", SqlParserPos.ZERO));
    } else if (operands.size() == 2) {
      return formatDatetime.createCall(SqlParserPos.ZERO, fromUnixtime.createCall(SqlParserPos.ZERO, operands.get(0)),
          operands.get(1));
    }

    return sqlCall;
  }
}

/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.spark.transformers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.linkedin.coral.common.transformers.SqlCallTransformer;

import static org.apache.calcite.rel.rel2sql.SqlImplementor.*;


/**
 * Transformer to add explicit alias names if the following conditions are met:
 * 1. `aliases` is provided
 * 2. Input sqlCall is not SELECT *
 * 3. Input sqlCall is the outermost SELECT
 */
public class AddExplicitAliasTransformer extends SqlCallTransformer {

  private final List<String> aliases;
  // Use a boolean to track if it's the outermost select statement
  private boolean isOutermostLevel;

  public AddExplicitAliasTransformer(List<String> aliases) {
    this.aliases = aliases;
    this.isOutermostLevel = true;
  }

  @Override
  protected boolean condition(SqlCall sqlCall) {
    return aliases != null && isOutermostLevel && sqlCall.getKind() == SqlKind.SELECT
        && ((SqlSelect) sqlCall).getSelectList() != null;
  }

  @Override
  protected SqlCall transform(SqlCall sqlCall) {
    isOutermostLevel = false;
    SqlSelect select = (SqlSelect) sqlCall;
    // Make sure the select list is the same length as the coral-schema fields
    Preconditions.checkState(aliases.size() == select.getSelectList().size());
    List<SqlNode> aliasedSelectNodes = IntStream.range(0, select.getSelectList().size())
        .mapToObj(i -> updateAlias(select.getSelectList().get(i), aliases.get(i))).collect(Collectors.toList());
    select.setSelectList(new SqlNodeList(aliasedSelectNodes, SqlParserPos.ZERO));
    return select;
  }

  private SqlNode updateAlias(SqlNode node, String newAlias) {
    if (node.getKind() == SqlKind.AS) {
      // If alias already exists, replace it with the new one
      SqlNode selectWithoutAlias = ((SqlCall) node).getOperandList().get(0);
      return SqlStdOperatorTable.AS.createCall(POS, selectWithoutAlias, new SqlIdentifier(newAlias, POS));
    } else {
      // If there's no existing alias, just add the new alias
      return SqlStdOperatorTable.AS.createCall(POS, node, new SqlIdentifier(newAlias, POS));
    }
  }
}

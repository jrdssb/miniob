/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#pragma once

#include <memory>

#include "common/rc.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"

class Table;
class FilterStmt;

/**
 * @brief 更新语句
 * @ingroup Statement
 */
class UpdateStmt : public Stmt
{
public:
  UpdateStmt() = default;
  //新建updateStmt需要多一个change_val参数
  UpdateStmt(Table *table,Field field, Value value, FilterStmt *filter_stmt, std::vector<Value> change_vals);

  StmtType type() const override{
    return StmtType::UPDATE;
  }

public:
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);

public:
  Table *table() const 
  { 
    return table_; 
  }
  const Field field() const 
  { 
    return field_; 
  }
  const Value value() const 
  {
    return value_; 
  }
  FilterStmt *filter_stmt() const 
  {
    return filter_stmt_; 
  }
  //获取change_val的方法
  const std::vector<Value> change_vals(){
    return change_vals_;
  }

private:
  Table *table_        = nullptr;
  Field field_;
  Value value_ ;
  FilterStmt *filter_stmt_=nullptr;
  //在创建stmt时保存所有条件中的value值，传入logical中
  std::vector<Value> change_vals_;
};

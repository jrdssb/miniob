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

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "filter_stmt.h"
#include "storage/db/db.h"

UpdateStmt::UpdateStmt(Table *table,Field field, Value value, FilterStmt *filter_stmt, std::vector<Value> change_vals)
    : table_(table), field_(field) , value_(value), filter_stmt_(filter_stmt), change_vals_(change_vals)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt)
{
  // TODO
  if(db==nullptr){
    LOG_WARN("invalid argument, db is null");
    return RC::INVALID_ARGUMENT;
  }

  const char*table_name=update_sql.relation_name.c_str();
  //check where the table exists
  if(table_name==nullptr){
    LOG_WARN("invalid argument, realtion name is null");
    return RC::INVALID_ARGUMENT;
  }

  //定义table
  Table*table=db->find_table(table_name);
  if(table==nullptr){
    LOG_WARN("no such table");
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  const FieldMeta*field_meta=table->table_meta().field(update_sql.attribute_name.c_str());

  if(nullptr==field_meta){
    LOG_WARN("no such field.field");
    return RC::SCHEMA_FIELD_MISSING;
  }

  std::unordered_map<std::string,Table*> table_map;
  table_map.insert(std::pair<std::string,Table*>(table_name,table));

  //filter定义
  FilterStmt *filter_stmt=nullptr;
  RC rc=FilterStmt::create(
    db,
    table,
    &table_map,
    update_sql.conditions.data(),
    static_cast<int>(update_sql.conditions.size()),
    filter_stmt
  );
  if(filter_stmt==nullptr){
    LOG_WARN("no filter");
    return RC::INVALID_ARGUMENT;
  }
  
  Field* field0=new Field(table,field_meta);
  Field field=*field0;

  std::vector<FilterUnit *> fus=filter_stmt->filter_units();

  //获取所有filter中的值，把他们传入updatestmt当中,后续与找到的位置进行对比，如果get_int相同就应该修改
  std::vector<Value> change_vals;
  for(FilterUnit*fu:fus){
    Value v;
    //假设filter中所有value都在right侧
    v.set_type(fu->right().value.attr_type());
    //将值作为int类型，因为后续只需要比较int是否相同，所以暂时不考虑数据类型
    v.set_int(fu->right().value.get_int());
    change_vals.push_back(v);
  }
  stmt = new UpdateStmt(table, field, update_sql.value, filter_stmt, change_vals);
  return rc;
}

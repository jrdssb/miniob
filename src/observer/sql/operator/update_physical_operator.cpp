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
// Created by WangYunlai on 2021/6/9.
//

#include "sql/operator/update_physical_operator.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table,Field field,Value value,std::vector<Value> change_vals)
    :table_(table),field_(field),value_(value),change_vals_(change_vals)
{
}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
    RC rc=RC::SUCCESS;
    if(children_.empty()){
        return RC::RECORD_EOF;
    }

    PhysicalOperator *child = children_[0].get();
    //每次循环会处理一个要输出的field_meta
    while(RC::SUCCESS == (rc=child->next())){
        Tuple *tuple =child->current_tuple();
        if(nullptr==tuple){
            LOG_WARN("failed to get current record %s",strrc(rc));
            return rc;
        }
        RowTuple *row_tuple= static_cast<RowTuple*>(tuple);
        Record& record=row_tuple->record();

       const char*field_name=field_.field_name();
       const FieldMeta *field_meta=table_->table_meta().field(field_name);
       
       //需要检查更新数据类型与原数据类型是否相同，不同需要输出failure
       if(field_meta->type()!=value_.attr_type()){
            return RC::UNIMPLENMENT;
       }

       int offset_=field_meta->offset();
       int len_=field_meta->len();
      
      //这里是获取当前需要修改记录原位置的值
      Value cell;
      cell.set_type(field_meta->type());
      cell.set_data(record.data() + field_meta->offset(), field_meta->len());
      //std::cout<<"in physical plan, original int is"<<cell.get_int()<<std::endl;

      //测试是否成功获得filter value数组
      std::cout<<"test filter values:";
      for(Value v:change_vals_){
        //std::cout<<v.get_int()<<" ";
      }
      std::cout<<std::endl;

       rc=trx_->update_record(table_, record, offset_, len_ , value_);
       if(rc!=RC::SUCCESS){
        LOG_WARN("failed to ge delete record :%s",strrc(rc));
        return rc;
       }
    }
    return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
#pragma once
#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"

/**
*@brief 更新逻辑算子
*@ingroup LogicalOperator
*/

class UpdateLogicalOperator:public LogicalOperator{
public:
    UpdateLogicalOperator(Table *table, Field field, Value value, std::vector<Value> change_vals);
    virtual ~UpdateLogicalOperator()=default;

    LogicalOperatorType type()const override{
        return LogicalOperatorType::UPDATE;
    }

    Table *table() const{ return table_; }
    const Field field() const { return field_; }
    const Value value() const { return  value_; }
    const std::vector<Value> change_vals(){return change_vals_;}

private:
    Table *table_=nullptr;
    Field field_;
    Value value_;
    std::vector<Value> change_vals_;
};
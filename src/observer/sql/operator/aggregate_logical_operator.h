#pragma once

#include<memory>
#include<vector>

#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "storage/field/field.h"

class AggregateLogicalOperator: public LogicalOperator{
    /*aggr->...->logical_plan_generator.cpp*/
    public:
    AggregateLogicalOperator(const std::vector<Field>&field);
    virtual ~AggregateLogicalOperator()=default;

    LogicalOperatorType type() const override{
        return LogicalOperatorType::AGGREGATE;
    }

    const std::vector<Field> &fields() const{
        return fields_;
    }

    private:
    //aggregate语句涉及的对象为字段和聚合函数类型，聚合函数类型已经被定义在字段当中，因此只需要定义字段变量
        std::vector<Field> fields_;
};

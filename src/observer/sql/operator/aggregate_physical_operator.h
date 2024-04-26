#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include "sql/expr/tuple.h"

/**
 * @brief 聚合物理算子
 * @ingroup PhysicalOperator
*/

class AggregatePhysicalOperator : public PhysicalOperator{
//该类会在创建aggregate物理算子时创建，而聚合是对最终的到的元组中=某些字段进行操作的过程
public:
    //构造函数
    AggregatePhysicalOperator(){}

    virtual ~AggregatePhysicalOperator()=default;

    //add_aggregation:在创建物理逻辑算子时会根据logical_aggregate中的aggregatrion参数加入
    void add_aggregation(const AggrOp aggregation);

    PhysicalOperatorType type() const override
    {
        return PhysicalOperatorType::AGGREGATE;
    }

    RC open(Trx *trx)override;
    RC next() override;
    RC close() override;

    Tuple *current_tuple() override;

    std::vector<AggrOp> getAggrType() {  
        return aggregations_;  
    } 

private:
    std::vector<AggrOp> aggregations_;
    ValueListTuple result_tuple_;//用作aggregate操作是否完成的标记--fnmdp,标记牛魔，应该是处理结果之后的返回tuple
};

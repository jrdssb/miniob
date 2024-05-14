#include "sql/operator/update_logical_operator.h"

UpdateLogicalOperator::UpdateLogicalOperator(Table *table,Field field,Value value, std::vector<Value> change_vals)
    :table_(table),field_(field),value_(value),change_vals_(change_vals)
{
}
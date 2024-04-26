#include<iostream>
#include<cfloat>
#include "sql/operator/aggregate_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/parser/value.h"

RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

//next会获取每个属性及其操作
RC AggregatePhysicalOperator::next(){
    //std::cout<<"stage6 aggregate pysical generate next() function"<<std::endl;
    //already aggregate
    
    //这里把result_tuple作为结束的aggregate完成的标志

    if(result_tuple_.cell_num()>0){
        return RC::RECORD_EOF;
    }

    RC rc=RC::SUCCESS;
    PhysicalOperator *oper=children_[0].get();

    std::vector<Value> result_cells;

    //结果向量
    float*series_float=new float[(int)aggregations_.size()];
    
    for(int i=0;i<(int)aggregations_.size();i++){
      series_float[i]=0;
    }

    //while每次取一条记录，也就是说while会访问一个属性所有元
    float avg_total=0;
    float count=0;
    float avg=0;
    float temp=0;
    float min=FLT_MAX;
    float max=FLT_MIN;

    while(RC::SUCCESS==(rc=oper->next())){
        //get tuple
        //这里调用了project算子的current_tuple函数会返回project算子中的ProjectTuple变量,同时会执行set_tuple函数
        Tuple* tuple=oper->current_tuple();

        //do aggregate
        for(int cell_idx=0;cell_idx<(int)aggregations_.size();cell_idx++){

            const AggrOp aggregation = aggregations_[cell_idx];
            Value cell;
            AttrType attr_type=AttrType::INTS;

            switch(aggregation){
                case AggrOp::AGGR_SUM:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        temp+=cell.get_float();
                        series_float[cell_idx]+=cell.get_float();
                    }
                    break;
                case AggrOp::AGGR_AVG:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        count+=1;
                        avg_total+=cell.get_float();
                        avg=avg_total/count;
                        series_float[cell_idx]=avg;
                    }
                    break;
                case AggrOp::AGGR_MAX:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        float tempCell_float=cell.get_float();
                        if(tempCell_float>max){
                          series_float[cell_idx]=tempCell_float;
                          max=tempCell_float;
                        }
                    }
                    break;
                case AggrOp::AGGR_MIN:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        float tempCell_float=cell.get_float();
                        if(tempCell_float<min){
                            series_float[cell_idx]=tempCell_float;
                            min=tempCell_float;
                        }
                    }
                    break;
                case AggrOp::AGGR_COUNT:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        series_float[cell_idx]+=1;
                    }
                    break;
                case AggrOp::AGGR_COUNT_ALL:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        series_float[cell_idx]+=1;
                    }
                    break;
                default:
                    return RC::UNIMPLENMENT;
            }
        }
    }
    if(rc==RC::RECORD_EOF){
        //std::cout<<"series_float array is:";
        for(int i=0;i<(int)aggregations_.size();i++){
          //<<series_float[i]<<" ";
          Value *p=new Value();
          result_cells.push_back(*p);
          result_cells[i].set_float(series_float[i]);
        }
        //std::cout<<std::endl;
        result_tuple_.set_cells(result_cells);
        rc=RC::SUCCESS;
    }
    result_tuple_.set_cells(result_cells);
    return rc;
}

RC AggregatePhysicalOperator::close()
{
  //std::cout<<"judge whether to close"<<std::endl;
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

Tuple *AggregatePhysicalOperator::current_tuple()
{
  //std::cout<<"need to return result tuple"<<std::endl;
  // Value *p=new Value();
  // result_tuple_.cell_at(0,*p);
  // std::cout<<p->get_float()<<std::endl;
  return &result_tuple_;
  //current会起到显示最后结果的作用
}

void AggregatePhysicalOperator::add_aggregation(const AggrOp aggrop)
{
  //std::cout<<"need to add an aggregation operator"<<std::endl;
  aggregations_.push_back(aggrop);
  //add_aggregation起到什么作用？
}

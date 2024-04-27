#include<iostream>
#include<cfloat>
#include "sql/operator/aggregate_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include "sql/parser/value.h"

void int2Str(std::string &strDate,int intDate){
  int temp=0;
  temp=intDate/10000;
  strDate+=std::to_string(temp)+"-";
  temp=(intDate%10000)/100;
  if(temp<10)
    strDate+="0"+std::to_string(temp)+"-";
  else
    strDate+=std::to_string(temp)+"-";
  temp=intDate%100;
  if(temp<10)
    strDate+="0"+std::to_string(temp);
  else
    strDate+=std::to_string(temp);
}

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

    //结果向量,为了支持char和date，需要将float类型改为Value
    Value* value_series =new Value[(int)aggregations_.size()];
    
    for(int i=0;i<(int)aggregations_.size();i++){
      //这里需要set
      value_series[i].set_float(0);
    }

    //while每次取一条记录，也就是说while会访问一个属性所有元
    float avg_total=0;
    float count=0;
    float avg=0;
    float temp=0;
    float min=FLT_MAX;
    float max=FLT_MIN;
    //用来保存date结果
    std::string date_string="";

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
                        value_series[cell_idx].set_float(cell.get_float());
                    }
                    break;
                case AggrOp::AGGR_AVG:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        count+=1;
                        avg_total+=cell.get_float();
                        avg=avg_total/count;
                        value_series[cell_idx].set_float(avg);
                    }
                    break;
                //理论上date和char类型数据没办法求平均或者求和，同时count不受属性影响，因此先实现最大最小的比较
                case AggrOp::AGGR_MAX:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        float tempCell_float=cell.get_float();
                        if(tempCell_float>max){
                          value_series[cell_idx].set_float(tempCell_float);
                          max=tempCell_float;
                        }
                    }else if (attr_type==AttrType::DATES){
                        //获得int类型的日期
                        int temp_date=cell.get_date();
                        //与max比较，如果大执行以下步骤
                        if(temp_date>max){
                          max=temp_date;
                          //int2Str(date_string,temp_date);
                          //调用函数转化为string并set到value_series当中
                          //const char* cStr = date_string.c_str();  
                          value_series[cell_idx].set_date(temp_date);
                        }

                    }
                    //else if (attr_type==AttrType::CHARS){
                      //request：需要实现char
                    //}
                    break;
                case AggrOp::AGGR_MIN:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        float tempCell_float=cell.get_float();
                        if(tempCell_float<min){
                            value_series[cell_idx].set_float(tempCell_float);
                            min=tempCell_float;
                        }
                    }else if (attr_type==AttrType::DATES){
                        //获得int类型的日期
                        int temp_date=cell.get_date();
                        //与max比较，如果大执行以下步骤
                        if(temp_date<min){
                          max=temp_date;
                          //int2Str(date_string,temp_date);
                          //调用函数转化为string并set到value_series当中
                          //const char* cStr = date_string.c_str();
                          value_series[cell_idx].set_date(temp_date);
                        }

                    }
                    break;
                case AggrOp::AGGR_COUNT:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        value_series[cell_idx].set_float(value_series[cell_idx].get_float()+1);
                    }
                    break;
                case AggrOp::AGGR_COUNT_ALL:
                    rc=tuple->cell_at(cell_idx,cell);
                    attr_type=cell.attr_type();

                    if(attr_type==AttrType::INTS or attr_type == AttrType::FLOATS){
                        value_series[cell_idx].set_float(value_series[cell_idx].get_float()+1);
                    }
                    break;
                default:
                    return RC::UNIMPLENMENT;
            }
        }
    }
    if(rc==RC::RECORD_EOF){
        //std::cout<<"value_series  array is:";
        for(int i=0;i<(int)aggregations_.size();i++){
          //<<value_series [i]<<" ";
          Value *p=new Value();
          result_cells.push_back(*p);
          if(value_series[i].attr_type()==AttrType::INTS or value_series[i].attr_type()==AttrType::FLOATS)
            result_cells[i].set_float(value_series[i].get_float());
          else if(value_series[i].attr_type()==AttrType::DATES){
            result_cells[i].set_date(value_series[i].get_date());
          }
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

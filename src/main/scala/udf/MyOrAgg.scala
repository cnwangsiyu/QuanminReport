package udf

/**
  * Created by WangSiyu on 15/03/2017.
  */

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{IntegerType, StructType}

class MyOrAgg extends UserDefinedAggregateFunction {
  override def inputSchema = new StructType().
    add("nums", IntegerType)

  override def bufferSchema = new StructType().
    add("result", IntegerType)

  override def dataType = IntegerType

  override def deterministic = true

  override def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row) = {
    buffer(0) = myOr(buffer.getInt(0), input.getInt(0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = myOr(buffer1.getInt(0), buffer2.getInt(0))
  }

  override def evaluate(buffer: Row) = {
    buffer.getInt(0)
  }

  def myOr(num1: Integer, num2: Integer) = {
    if (num1 == 0 && num2 == 0) {
      0
    } else {
      1
    }
  }
}

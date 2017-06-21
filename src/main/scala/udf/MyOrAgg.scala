package udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
  * Created by WangSiyu on 15/03/2017.
  */
class MyOrAgg extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = new StructType().
    add("nums", IntegerType)

  override def bufferSchema: StructType = new StructType().
    add("result", IntegerType)

  override def dataType = IntegerType

  override def deterministic = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = myOr(buffer.getInt(0), input.getInt(0))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = myOr(buffer1.getInt(0), buffer2.getInt(0))
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getInt(0)
  }

  def myOr(num1: Integer, num2: Integer): Integer = {
    if (num1 == 0 && num2 == 0) {
      0
    } else {
      1
    }
  }
}


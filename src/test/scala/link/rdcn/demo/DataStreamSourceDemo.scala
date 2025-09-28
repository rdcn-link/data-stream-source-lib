package link.rdcn.demo

import link.rdcn.struct.{ClosableIterator, DataStreamSource, Row, StructType, ValueType}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/9/28 19:45
 * @Modified By:
 */
class DataStreamSourceDemo extends DataStreamSource{

  override def rowCount: Long = 2L

  override def schema: StructType = StructType.empty.add("id", ValueType.IntType)
    .add("name", ValueType.StringType)

  override def iterator: ClosableIterator[Row] = {
    val iter = Seq(Row.fromSeq(Seq(1,"Jack")), Row.fromSeq(Seq(1,"Tom"))).iterator
    ClosableIterator(iter)()
  }
}

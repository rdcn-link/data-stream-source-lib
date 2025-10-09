package link.rdcn.utils

import link.rdcn.struct.{ClosableIterator, DataStreamSource, Row, StructType}

class NetCdfUtil {

  def netcdf(netcdfPath: String): DataStreamSource = {
    import ucar.nc2.dataset.NetcdfDataset
    import scala.collection.JavaConverters._

    val netcdfFile = NetcdfDataset.openDataset(netcdfPath)

    // 获取所有变量
    val variables = netcdfFile.getVariables.asScala.toList

    // 更智能地确定记录数 - 查找时间维度或其他主维度
    val recordCount = if (variables.nonEmpty) {
      // 尝试查找时间维度或主维度
      val timeDimVars = variables.filter(_.getShortName.toLowerCase.contains("time"))
      if (timeDimVars.nonEmpty) {
        // 如果有时间变量，使用时间变量的长度作为记录数
        val timeVar = timeDimVars.head
        val shape = timeVar.getShape
        if (shape.length > 0) shape(0).toLong else 1L
      } else {
        // 否则查找最大维度作为记录数参考
        val maxDimSize = variables.map { variable =>
          val shape = variable.getShape
          if (shape.length > 0) shape(0).toLong else 0L
        }.max
        maxDimSize
      }
    } else {
      0L
    }

    // 创建数据迭代器，优化数据读取方式
    val iterRows = new Iterator[Row] {
      // 缓存变量数据以提高性能
      private val variableDataCache = scala.collection.mutable.Map[String, ucar.ma2.Array]()
      private var currentIndex = 0

      override def hasNext: Boolean = currentIndex < recordCount

      override def next(): Row = {
        val values = variables.flatMap { variable =>
          try {
            val shape = variable.getShape
            val dataType = variable.getDataType
            val varName = variable.getShortName

            if (shape.length == 0) {
              // 标量变量
              val dataArray = variable.read()
              val value = dataArray.getObject(0).toString
              List(value)
            } else if (shape.length == 1) {
              // 一维变量 - 直接读取整个数组然后取特定索引
              val dataArray = variableDataCache.getOrElseUpdate(varName, variable.read())
              val value = if (currentIndex < dataArray.getSize) {
                dataType match {
                  case ucar.ma2.DataType.INT => dataArray.getInt(currentIndex)
                  case ucar.ma2.DataType.LONG => dataArray.getLong(currentIndex)
                  case ucar.ma2.DataType.FLOAT => dataArray.getFloat(currentIndex)
                  case ucar.ma2.DataType.DOUBLE => dataArray.getDouble(currentIndex)
                  case ucar.ma2.DataType.STRING => dataArray.getObject(currentIndex).toString
                  case ucar.ma2.DataType.CHAR => dataArray.getObject(currentIndex).toString
                  case _ => dataArray.getObject(currentIndex).toString
                }
              } else {
                null
              }
              List(value)
            } else {
              // 多维变量 - 按第一个维度索引读取对应的切片
              val dataArray = variable.read(s"$currentIndex,:" * (shape.length - 1))

              // 将多维数据展开为一维列表
              val flatValues = (0 until dataArray.getSize.toInt).map { i =>
                dataType match {
                  case ucar.ma2.DataType.INT => dataArray.getInt(i)
                  case ucar.ma2.DataType.LONG => dataArray.getLong(i)
                  case ucar.ma2.DataType.FLOAT => dataArray.getFloat(i)
                  case ucar.ma2.DataType.DOUBLE => dataArray.getDouble(i)
                  case ucar.ma2.DataType.STRING => dataArray.getObject(i).toString
                  case ucar.ma2.DataType.CHAR => dataArray.getObject(i).toString
                  case _ => dataArray.getObject(i).toString
                }
              }
              flatValues.toList
            }
          } catch {
            case _: Exception =>
              val shape = variable.getShape
              if (shape.length <= 1) {
                List(null)
              } else {
                val elementCount = shape.drop(1).product // 除第一个维度外的元素数
                List.fill(elementCount)(null)
              }
          }
        }
        currentIndex += 1
        Row.fromSeq(values)
      }
    }

    new DataStreamSource {
      override def rowCount: Long = recordCount

      override def schema: StructType = StructType.binaryStructType

      override def iterator: ClosableIterator[Row] = ClosableIterator(iterRows) { () =>
        // 清理缓存
        // variableDataCache.clear()
        if (netcdfFile != null) {
          netcdfFile.close()
        }
      }
    }
  }

}

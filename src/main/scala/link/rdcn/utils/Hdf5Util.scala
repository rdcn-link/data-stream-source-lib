package link.rdcn.utils

import io.jhdf.api.{Dataset, Node}
import link.rdcn.struct.{ClosableIterator, DataStreamSource, Row, StructType}

import scala.reflect.ClassTag

class Hdf5Util {

  def hdf5(hdf5Path: String): DataStreamSource = {
    import io.jhdf.HdfFile
    import io.jhdf.api.Dataset
    import io.jhdf.api.Node
    import java.util.{Map => JMap}
    import scala.collection.JavaConverters._

    val hdfFile = new HdfFile(new java.io.File(hdf5Path))

    // 获取所有数据集
    val datasets = collectDatasets(hdfFile)

    // 更准确地计算记录数 - 查找共享维度
    val recordCount = if (datasets.nonEmpty) {
      // 查找所有数据集中最小的第一维度作为记录数
      datasets.map { case (_, dataset) =>
        val dims = dataset.getDimensions
        if (dims.length > 0) dims(0).toLong else 1L
      }.min
    } else {
      0L
    }

    // 创建数据迭代器
    val iterRows = new Iterator[Row] {
      private var currentIndex = 0

      override def hasNext: Boolean = currentIndex < recordCount

      override def next(): Row = {
        val values = datasets.flatMap { case (name, dataset) =>
          try {
            val dims = dataset.getDimensions

            if (dims.length == 0) {
              // 标量数据集
              List(dataset.getData.toString)
            } else if (dims.length == 1) {
              // 一维数据集
              val javaArray = dataset.getData
              val array = javaArray.asInstanceOf[Array[_]]
              if (currentIndex < array.length) {
                List(array(currentIndex).toString)
              } else {
                List(null)
              }
            } else {
              // 多维数据集 - 添加数据量检查
              val totalElements = dims.drop(1).product
              if (totalElements > 1000000) { // 限制切片大小
                println(s"警告: 数据集 $name 切片过大 ($totalElements 元素)，跳过处理")
                List.fill(totalElements.toInt)(null)
              } else {
                // 多维数据集 - 构造索引字符串直接读取特定切片
                // 使用jhdf的索引功能，避免读取整个数据集
                val indexSpec = createIndexSpec(currentIndex, dims.length)
                val slicedData = readSliceData(dataset, indexSpec)
                if (slicedData != null) {
                  // 将切片数据扁平化
                  val flatArray = flattenArray(slicedData)
                  flatArray.map(_.toString).toList
                } else {
                  List.fill(totalElements.toInt)(null)
                }
              }
            }
          } catch {
            case _: Exception =>
              val dims = dataset.getDimensions
              if (dims.length <= 1) {
                List(null)
              } else {
                // 对于多维数据集，计算切片元素数量
                val sliceElementCount = dims.drop(1).product
                List.fill(sliceElementCount.toInt)(null)
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
        if (hdfFile != null) {
          hdfFile.close()
        }
      }
    }
  }

  // 辅助方法：创建索引规范字符串
  private def createIndexSpec(firstIndex: Int, dimensions: Int): String = {
    // 生成类似 "5,:,:" 的索引字符串
    firstIndex + (",:" * (dimensions - 1))
  }

  // 辅助方法：读取切片数据
  private def readSliceData(dataset: Dataset, indexSpec: String): AnyRef = {
    try {
      // 使用 jhdf 提供的 getData 方法，传入索引规范
      // 检查 Dataset 是否有支持索引读取的方法
      val getDataMethod = dataset.getClass.getMethod("getData", classOf[String])
      getDataMethod.invoke(dataset, indexSpec).asInstanceOf[AnyRef]
    } catch {
      case _: Exception =>
        // 如果索引读取失败，返回 null 而不是读取完整数据
        null
    }
  }

  // 辅助方法：根据第一维度索引提取多维数组切片
  private def extractSliceByFirstDimension(array: AnyRef, firstIndex: Int, dims: Array[Long]): AnyRef = {
    // 对于多维数组，根据第一个维度索引提取子数组
    // 这是一个简化的实现，实际可能需要更复杂的数组操作
    array match {
      case arr: Array[_] if arr.length > firstIndex =>
        arr(firstIndex).asInstanceOf[AnyRef]
      case _ => null
    }
  }

  // 辅助方法：收集所有数据集
  private def collectDatasets(node: Node): List[(String, Dataset)] = {
    import io.jhdf.api.Group
    import io.jhdf.api.Dataset
    import scala.collection.JavaConverters._

    node match {
      case dataset: Dataset => List((dataset.getName, dataset))
      case group: Group =>
        group.getChildren.values.asScala.toList.flatMap(collectDatasets)
      case _ => List.empty
    }
  }

  // 辅助方法：将多维数组扁平化
  private def flattenArray(obj: AnyRef): Array[Any] = {
    obj match {
      case array: Array[_] =>
        array.flatMap {
          case nestedArray: Array[_] => flattenArray(nestedArray)
          case element => Array[Any](element)
        }
      case _ => Array[Any](obj)
    }
  }
}

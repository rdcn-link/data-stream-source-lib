package link.rdcn.utils

import link.rdcn.struct.ValueType.{BlobType, LongType, RefType, StringType}
import link.rdcn.struct.{Blob, ClosableIterator, DFRef, DataStreamSource, Row, StructType}
import link.rdcn.util.DataUtils

import java.io.File
import java.nio.file.attribute.BasicFileAttributes

/**
 * @Author renhao
 * @Description:
 * @Data 2025/10/9 16:42
 * @Modified By:
 */
object FileSystemLoader {

  def loadDirectory(file: File): DataStreamSource = {
    var iterFiles: Iterator[(File, BasicFileAttributes)] = Iterator.empty
    iterFiles = DataUtils.listFilesWithAttributes(file.getAbsoluteFile).toIterator
    val stream = iterFiles
      // schema [name, byteSize, 文件类型, 创建时间, 最后修改时间, 最后访问时间, file]
      .map { file =>
        val ref = {
          if(file._1.isFile) DFRef(registryFile(file._1))
          else DFRef(registryDirectory(file._1))
        }
        (file._1.getName, file._2.size(), DataUtils.getFileType(file._1),
        file._2.creationTime().toMillis, file._2.lastModifiedTime().toMillis,
        file._2.lastAccessTime().toMillis, ref) }
      .map(Row.fromTuple(_))
    new DataStreamSource {
      override def rowCount: Long = -1

      override def schema: StructType = StructType.empty.add("name", StringType).add("byteSize", LongType).add("type", StringType)
        .add("creationTime", LongType).add("lastModifiedTime", LongType).add("lastAccessTime", LongType)
        .add("File", RefType)

      override def iterator: ClosableIterator[Row] = new ClosableIterator(stream, () => {}, true)
    }
  }
  /**
   * 将file或者directory初始化为DataFrame
   * 1、为file或者directory资源定义名称，例如定义名称为：/path,则在DataProvider getDataStreamSource
   * 可以根据 /path 获取到 DataStreamSource
   * 2、return "dacp://ip:3101/path"
   * */
  private def registryFile(file: File): String = ???

  private def registryDirectory(file: File): String = ???

}

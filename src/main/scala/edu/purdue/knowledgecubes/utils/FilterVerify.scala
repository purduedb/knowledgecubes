package edu.purdue.knowledgecubes.utils

import java.io._

import edu.purdue.knowledgecubes.GEFI.{GEFI, GEFIType}

object FilterVerify {

  def main(args: Array[String]): Unit = {

    val path = "output/sample/GEFI/join/BLOOM/0.001/"
    val fileName = "example"
    val f = new File(path)
    f.mkdirs()

    val filter = new GEFI(GEFIType.ROARING, 5, 0)
    filter.add(1)
    filter.add(2)
    filter.add(3)

    val fout = new FileOutputStream(path + fileName)
    val oos = new ObjectOutputStream(fout)
    oos.writeObject(filter)
    oos.close()

    val fin = new FileInputStream(path + fileName)
    val ois = new ObjectInputStream(fin)
    val filter2 = ois.readObject.asInstanceOf[GEFI]

    println(filter2.contains(1))

  }

}

package cvm_insf

object Arguments {
  def isArgsValid(args: Array[String]): Boolean = {
    if (args.length < 7) false
    else true
  }

}

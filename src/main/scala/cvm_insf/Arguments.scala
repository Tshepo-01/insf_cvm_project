package cvm_insf

object Arguments {
  def isArgsValid(args: Array[String]): Boolean = {
    if (args.length < 9) false
    else true
  }

}

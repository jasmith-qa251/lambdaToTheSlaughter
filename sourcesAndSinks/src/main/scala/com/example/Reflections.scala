package com.example

import scala.reflect.runtime.{universe => ru}

/**
  * This is to invoke the method which has been defined in the scala object using reflection at runtime.
  */
object Reflections {

  /**
    * This is to invoke a method which has been defined in a scala object using reflection at runtime.
    *
    * @param objectName - object name to be invoked via reflection
    * @param methodName - method name which should have been defined in the above object.
    * @param inParam - key-value pair of parameters to be passed into the above method.
    * @tparam T - Generic type - this is to enforce method return type at runtime.
    *               possible value - Dataframe if objectName is source,method Or Unit if objectName is Sink type
    * @return
    */
  def getObjectMethodNew[T](objectName: String, methodName: String, inParam: Map[String, Any]): T = {

    val rm = ru.runtimeMirror(getClass.getClassLoader)
    val module = rm.staticModule(objectName)
    val im = rm.reflectModule(module)
    val methodSymbol = im.symbol.info.decl(ru.TermName(methodName)).asMethod
    val objMirror = rm.reflect(im.instance)

    val methMirror = objMirror.reflectMethod(methodSymbol)

    val inputParamList: List[Any] = methMirror.symbol.paramLists.head.map(e => inParam(e.name.toString))

    methMirror(inputParamList: _*).asInstanceOf[T]
  }
}

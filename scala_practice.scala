package com.datamodeling.sparksql

import scala.io.StdIn

object scala_practice {

  def main(args: Array[String]): Unit = {

    val name = StdIn.readLine("Hi , What is your name:")

    println(s"$name , tell me something interesting , say 'bye' to the end talk" )

    var timeToBye = false

    while(!timeToBye)
      timeToBye = StdIn.readLine(">") match {
        case "bye" => println("ok, Bye")
          true
        case _=> println("interesting.....")
          false
      }
  }



}

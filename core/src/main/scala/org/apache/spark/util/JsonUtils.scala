/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.util.Date
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JLong, JNothing, JString, JValue}


object JsonUtils {
  private implicit val format = DefaultFormats

  /** Return an option that translates JNothing to None */
  def jsonOption(json: JValue): Option[JValue] = {
    json match {
      case JNothing => None
      case value: JValue => Some(value)
    }
  }

  def mapToImmutableMap[K <: Any, V <: Any, NK <: Any, NV <: Any](
      m: ConcurrentHashMap[K, V])(fn: (K, V) => (NK, NV)): Map[NK, NV] = {
    m.entrySet().asScala.map { entry => fn(entry.getKey, entry.getValue)}.toMap
  }

  def mapToImmutableMap[K <: Any, V <: Any, NK <: Any, NV <: Any](
      m: mutable.Map[K, V])(fn: (K, V) => (NK, NV)): Map[NK, NV] = {
    m.map { case (key, value) => fn(key, value) }.toMap
  }

  def mapValuesToImmutableMap[K <: Any, V <: Any, NV <: Any](
      m: ConcurrentHashMap[K, V])(fn: V => NV): Map[K, NV] = {
    m.entrySet().asScala.map { entry => entry.getKey -> fn(entry.getValue) }.toMap
  }

  def mapValuesToImmutableMap[K <: Any, V <: Any, NV <: Any](
      m: mutable.Map[K, V])(fn: V => NV): Map[K, NV] = {
    m.mapValues(fn).toMap
  }

  def mapToStringKeyImmutableMap[K <: AnyVal, V <: Any](m: Map[K, V]): Map[String, V] = {
    m.map { entry => entry._1.toString -> entry._2 }
  }

  def nullableDateToJson(date: Date): JValue = {
    Option(date).map(_.getTime).map(JLong(_)).orNull
  }

  def jsonToDate(json: JValue, fieldName: String): Date = {
    Option(json \ fieldName).map(_.extract[Long]).map(new Date(_)).orNull
  }

  def optionDateToJson(date: Option[Date]): JValue = {
    optionLongToJson(date.map(_.getTime))
  }

  def optionStringToJson(str: Option[String]): JValue = {
    str.map(JString(_)).getOrElse(JNothing)
  }

  def optionLongToJson(v: Option[Long]): JValue = {
    v.map(JLong(_)).getOrElse(JNothing)
  }

  def jsonToOptionDate(json: JValue, fieldName: String): Option[Date] = {
    jsonOption(json \ fieldName).map(_.extract[Long]).map(new Date(_))
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.wherobots.sedona

import org.apache.iceberg.expressions
import org.apache.iceberg.expressions.{Expressions => IcebergExpressions}
import org.apache.iceberg.spark.geo.spi.GeospatialLibrary
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.execution.datasources.{PushableColumn, PushableColumnBase}
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.sedona_sql.expressions.{ST_Contains, ST_CoveredBy, ST_Covers, ST_Disjoint, ST_Intersects, ST_Predicate, ST_Within}

class SedonaGeospatialLibrary extends GeospatialLibrary {
  override def getGeometryType: DataType = GeometryUDT

  override def fromJTS(geometry: Geometry): AnyRef = GeometryUDT.serialize(geometry)

  override def toJTS(datum: Any): Geometry = GeometryUDT.deserialize(datum)

  override def isSpatialFilter(expression: Expression): Boolean = expression match {
    case pred: ST_Predicate => !pred.isInstanceOf[ST_Disjoint]
    case _ => false
  }

  override def translateToIceberg(expression: Expression): expressions.Expression = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled = true)
    expression match {
      case ST_Intersects(_) | ST_Contains(_) | ST_Covers(_) | ST_Within(_) | ST_CoveredBy(_) =>
        val icebergExpr = {
          for ((name, value) <- resolveNameAndLiteral(expression.children, pushableColumn))
            yield IcebergExpressions.stIntersects(unquote(name), GeometryUDT.deserialize(value))
        }
        icebergExpr.orNull
      case _ => null
    }
  }

  private def unquote(name: String): String = {
    name // TODO: Implement unquoting
  }

  private def resolveNameAndLiteral(expressions: Seq[Expression], pushableColumn: PushableColumnBase): Option[(String, Any)] = {
    expressions match {
      case Seq(pushableColumn(name), Literal(v, _)) => Some(name, v)
      case Seq(Literal(v, _), pushableColumn(name)) => Some(name, v)
      case _ => None
    }
  }
}

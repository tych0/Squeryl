/*******************************************************************************
 * Copyright 2010 Maxime Lévesque
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************** */
package org.squeryl;

import dsl.ast._
import dsl.{CompositeKey, QueryDsl}
import internals._
import java.sql.{Statement}
import logging.StackMarker
import scala.reflect.Manifest
import collection.mutable.ArrayBuffer
import javax.swing.UIDefaults.LazyValue

//private [squeryl] object DummySchema extends Schema

class Table[T] private [squeryl] (n: String, c: Class[T], val schema: Schema, _prefix: Option[String]) extends View[T](n, c, schema, _prefix) {

  private def _dbAdapter = Session.currentSession.databaseAdapter

  private def hasDbManagedFields: Boolean =
    _dbAdapter.supportsReturningClause && posoMetaData.dbManagedFields.headOption.isDefined

  def insert(t: T): T = StackMarker.lastSquerylStackFrame {

    val o = _callbacks.beforeInsert(t.asInstanceOf[AnyRef])
    val sess = Session.currentSession
    val sw = new StatementWriter(_dbAdapter)
    _dbAdapter.writeInsert(t, this, sw)

    val st =
      (_dbAdapter.supportsAutoIncrementInColumnDeclaration, posoMetaData.primaryKey) match {
        case (true, a:Any) => sess.connection.prepareStatement(sw.statement, Statement.RETURN_GENERATED_KEYS)
        case (false, Some(Left(pk:FieldMetaData))) => {
          val autoIncPk = new Array[String](1)
          autoIncPk(0) = pk.columnName
          sess.connection.prepareStatement(sw.statement, autoIncPk)
        }
        case a:Any => sess.connection.prepareStatement(sw.statement)
      }        

    try {
      val executeResult = _dbAdapter.executeUpdateForInsert(sess, sw, st)

      if (executeResult) {
        if (hasDbManagedFields) {
          val rs = st.getResultSet
          try {
            if (! rs.next())
                internals.Utils.throwError("Expected data")
            for {
              (fmd, index) <- posoMetaData.dbManagedFields.zipWithIndex
            } {
              fmd.setFromResultSet(o, rs, index+1)
            }
          } finally {
            rs.close()
          }
          st.getMoreResults()
        } else {
          internals.Utils.throwError("Unexpected result set")
        }
      }

      val cnt = st.getUpdateCount

      //if (cnt != 1)
      if (cnt != 1 && ! hasDbManagedFields) // Work around PG JDBC bug
        org.squeryl.internals.Utils.throwError("failed to insert")

      posoMetaData.primaryKey match {
        case Some(Left(pk:FieldMetaData)) => if(pk.isAutoIncremented) {
          val rs = st.getGeneratedKeys
          try {
            assert(rs.next,
              "getGeneratedKeys returned no rows for the auto incremented\n"+
              " primary key of table '" + name + "' JDBC3 feature might not be supported, \n or"+
              " column might not be defined as auto increment")
            pk.setFromResultSet(o, rs, 1)
          }
          finally {
            rs.close
          }
        }
        case _ =>
      }
    }
    finally {
      st.close
    }
    
    val r = _callbacks.afterInsert(o).asInstanceOf[T]

    _setPersisted(r)

    r
  }

//  def insert(t: Query[T]) = org.squeryl.internals.Utils.throwError("not implemented")

  def insert(e: Iterable[T]):Unit =
    _batchedUpdateOrInsert(e, t => posoMetaData.fieldsMetaData.filter(fmd => !fmd.isAutoIncremented && fmd.isInsertable), true, false)

  /**
   * isInsert if statement is insert otherwise update
   */
  private def _batchedUpdateOrInsert(e: Iterable[T], fmdCallback: T => Iterable[FieldMetaData], isInsert: Boolean, checkOCC: Boolean):Unit = {
    
    val it = e.iterator

    if(it.hasNext) {

      val e0 = it.next
      val fmds = fmdCallback(e0)
      val sess = Session.currentSession
      val dba = _dbAdapter
      val sw = new StatementWriter(dba)
      val forAfterUpdateOrInsert = new ArrayBuffer[AnyRef]

      if(isInsert) {
        val z = _callbacks.beforeInsert(e0.asInstanceOf[AnyRef])
        _callbacks.beforeInsert(z)
        dba.writeInsert(z.asInstanceOf[T], this, sw)
      }
      else {
        val z = _callbacks.beforeUpdate(e0.asInstanceOf[AnyRef])
        _callbacks.beforeUpdate(z)
        dba.writeUpdate(z.asInstanceOf[T], this, sw, checkOCC)
      }
      
      val st = sess.connection.prepareStatement(sw.statement)

      try {
        dba.fillParamsInto(dba.convertParamsForJdbc(sw.paramsZ), st)
        st.addBatch

        var updateCount = 1

        while(it.hasNext) {
          val eN0 = it.next.asInstanceOf[AnyRef]
          val eN =
            if(isInsert)
              _callbacks.beforeInsert(eN0)
            else
              _callbacks.beforeUpdate(eN0)

          forAfterUpdateOrInsert.append(eN)

          var idx = 1
          fmds.foreach(fmd => {
            st.setObject(idx, dba.convertToJdbcValue(fmd.getNativeJdbcValue(eN)))
            idx += 1
          })
          st.addBatch
          updateCount += 1
        }

        val execResults = st.executeBatch

        if(checkOCC)
          for(b <- execResults)
            if(b == 0) {
              val updateOrInsert = if(isInsert) "insert" else "update"
              throw new StaleUpdateException(
                "Attemped to "+updateOrInsert+" stale object under optimistic concurrency control")
            }
      }
      finally {
        st.close
      }

      for(a <- forAfterUpdateOrInsert)
        if(isInsert)
          _callbacks.afterInsert(a)
        else
          _callbacks.afterUpdate(a)

    }
  }

  /**
   * Updates without any Optimistic Concurrency Control check 
   */
  def forceUpdate(o: T)(implicit ev: T <:< KeyedEntity[_]) =
    _update(o, false)
  
  def update(o: T)(implicit ev: T <:< KeyedEntity[_]):Unit =
    _update(o, true)

  def update(o: Iterable[T])(implicit ev: T <:< KeyedEntity[_]):Unit =
    _update(o, true)

  def forceUpdate(o: Iterable[T])(implicit ev: T <:< KeyedEntity[_]):Unit =
    _update(o, false)

  private def _update(o: T, checkOCC: Boolean) = {

    val dba = Session.currentSession.databaseAdapter
    val sw = new StatementWriter(dba)
    val o0 = _callbacks.beforeUpdate(o.asInstanceOf[AnyRef]).asInstanceOf[T]
    dba.writeUpdate(o0, this, sw, checkOCC)

    val cnt  = dba.executeUpdateAndCloseStatement(Session.currentSession, sw)

    if(cnt != 1) {
      if(checkOCC && posoMetaData.isOptimistic) {
        val version = posoMetaData.optimisticCounter.get.getNativeJdbcValue(o.asInstanceOf[AnyRef])
        throw new StaleUpdateException(
           "Object "+prefixedName + "(id=" + o.asInstanceOf[KeyedEntity[_]].id + ", occVersionNumber=" + version +
           ") has become stale, it cannot be updated under optimistic concurrency control")
      }
      else
        org.squeryl.internals.Utils.throwError("failed to update")
    }

    _callbacks.afterUpdate(o0.asInstanceOf[AnyRef])
  }

  private def _update(e: Iterable[T], checkOCC: Boolean):Unit = {

    def buildFmds(t: T): Iterable[FieldMetaData] = {
      val pkList = posoMetaData.primaryKey.getOrElse(
        org.squeryl.internals.Utils.throwError("method was called with " + posoMetaData.clasz.getName + " that is not a KeyedEntity[]")).fold(
        pkMd => List(pkMd),
        pkGetter => {
          // Just for side-effect...
          var fields: Option[List[FieldMetaData]]  = None
          Utils.createQuery4WhereClause(this, (t0:T) => {
            val ck = pkGetter.invoke(t0).asInstanceOf[CompositeKey]

            fields = Some(ck._fields.toList)

            new EqualityExpression(new InputOnlyConstantExpressionNode(1), new InputOnlyConstantExpressionNode(1))
          })

          fields getOrElse (internals.Utils.throwError("No PK fields found"))
        }
      )

      List(
        posoMetaData.fieldsMetaData.filter(fmd=> ! fmd.isIdFieldOfKeyedEntity && ! fmd.isOptimisticCounter && fmd.isUpdatable).toList,
        pkList,
        posoMetaData.optimisticCounter.toList
      ).flatten
    }

    _batchedUpdateOrInsert(e, buildFmds _, false, checkOCC)
  }
  
  def update(s: T =>UpdateStatement):Int = {

    val vxn = new ViewExpressionNode(this)
    vxn.sample =
       posoMetaData.createSample(FieldReferenceLinker.createCallBack(vxn))    
    val us = s(vxn.sample)
    vxn.parent = Some(us)

    var idGen = 0
    us.visitDescendants((node,parent,i) => {

      if(node.parent == None)
        node.parent = parent

      if(node.isInstanceOf[UniqueIdInAliaseRequired]) {
        val nxn = node.asInstanceOf[UniqueIdInAliaseRequired]
        nxn.uniqueId = Some(idGen)
        idGen += 1
      }
    })

    vxn.uniqueId = Some(idGen)

    val dba = _dbAdapter
    val sw = new StatementWriter(dba)
    dba.writeUpdate(this, us, sw)
    dba.executeUpdateAndCloseStatement(Session.currentSession, sw)    
  }

  def delete(q: Query[T]): Int = {

    val queryAst = q.ast.asInstanceOf[QueryExpressionElements]
    queryAst.inhibitAliasOnSelectElementReference = true

    val sw = new StatementWriter(_dbAdapter)
    _dbAdapter.writeDelete(this, queryAst.whereClause, sw)

    _dbAdapter.executeUpdateAndCloseStatement(Session.currentSession, sw)
  }

  def deleteWhere(whereClause: T => LogicalBoolean)(implicit dsl: QueryDsl): Int =
    delete(dsl.from(this)(t => dsl.where(whereClause(t)).select(t)))      

  def delete[K](k: K)(implicit ev: T <:< KeyedEntity[K], dsl: QueryDsl): Boolean  = {
    import dsl._
    val q = from(this)(a => dsl.where {
      FieldReferenceLinker.createEqualityExpressionWithLastAccessedFieldReferenceAndConstant(a.id, k)
    } select(a))

    lazy val z = q.headOption

    if(_callbacks.hasBeforeDelete) {
      z.map(x => _callbacks.beforeDelete(x.asInstanceOf[AnyRef]))
    }

    val deleteCount = this.delete(q)

    if(_callbacks.hasAfterDelete) {
      z.map(x => _callbacks.afterDelete(x.asInstanceOf[AnyRef]))
    }

    assert(deleteCount <= 1, "Query :\n" + q.dumpAst + "\nshould have deleted at most 1 row but has deleted " + deleteCount)
    deleteCount == 1
  }

  def insertOrUpdate(o: T)(implicit ev: T <:< KeyedEntity[_]): T = {
    if(o.isPersisted)
      update(o)
    else
      insert(o)
    o
  }
}

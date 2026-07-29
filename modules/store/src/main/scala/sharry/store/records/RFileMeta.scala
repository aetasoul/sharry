package sharry.store.records

import sharry.common.*
import sharry.store.doobie.*
import sharry.store.doobie.DoobieMeta.*

import doobie.*
import doobie.implicits.*
import scodec.bits.ByteVector

case class RFileMeta(
    id: Ident,
    created: Timestamp,
    mimetype: String,
    length: ByteSize,
    checksum: ByteVector
) {}

object RFileMeta {

  val table = fr"filemeta"

  object Columns {
    val id = Column("file_id")
    val created = Column("created")
    val mimetype = Column("mimetype")
    val length = Column("length")
    val checksum = Column("checksum")

    val all = List(id, created, mimetype, length, checksum)
  }

  def insert(r: RFileMeta): ConnectionIO[Int] =
    Sql
      .insertRow(
        table,
        Columns.all,
        sql"${r.id}, ${r.created}, ${r.mimetype}, ${r.length}, ${r.checksum}"
      )
      .update
      .run

  /** Updates mimetype/length only, deliberately leaving `checksum` untouched.
    *
    * Every caller of this computes attributes with `excludeSha256`, so `r.checksum` is
    * always empty here. Writing it would clobber a real checksum already persisted by the
    * background worker (see `updateChecksum`) if that worker happened to run first.
    * Callers that genuinely need to (re)set the checksum must go through
    * `updateChecksum`.
    *
    * Not composed with `insert` into a single-transaction "upsert" here: the two
    * independent writers (foreground request vs. background checksum worker, see
    * `sharry.store.doobie.AttributeStore`) can each find the row missing and both attempt
    * an insert, so the update-else-insert retry has to happen across separate
    * transactions at the `F` level, not inside one `ConnectionIO`.
    */
  def update(r: RFileMeta): ConnectionIO[Int] =
    Sql
      .updateRow(
        table,
        Columns.id.is(r.id),
        Sql.commas(
          Columns.mimetype.setTo(r.mimetype),
          Columns.length.setTo(r.length)
        )
      )
      .update
      .run

  def findById(id: Ident): ConnectionIO[Option[RFileMeta]] =
    Sql.selectSimple(Columns.all, table, Columns.id.is(id)).query[RFileMeta].option

  def updateCreated(id: Ident, created: Timestamp): ConnectionIO[Int] =
    Sql.updateRow(table, Columns.id.is(id), Columns.created.setTo(created)).update.run

  def updateChecksum(id: Ident, checksum: ByteVector): ConnectionIO[Int] =
    Sql.updateRow(table, Columns.id.is(id), Columns.checksum.setTo(checksum)).update.run

  def updateLength(id: Ident, length: ByteSize): ConnectionIO[Int] =
    Sql.updateRow(table, Columns.id.is(id), Columns.length.setTo(length)).update.run

  def delete(id: Ident): ConnectionIO[Int] =
    Sql.deleteFrom(table, Columns.id.is(id)).update.run
}

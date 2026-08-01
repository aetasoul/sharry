package sharry.store.doobie

import java.sql.SQLException

import cats.data.OptionT
import cats.effect.*
import cats.implicits.*

import sharry.common.*
import sharry.store.records.RFileMeta

import binny.*
import doobie.*
import doobie.implicits.*

final private[store] class AttributeStore[F[_]: Sync](xa: Transactor[F]) {
  private val logger = sharry.logging.getLogger[F]

  def saveAttr(id: BinaryId, attrs: F[BinaryAttributes]): F[Unit] =
    for {
      now <- Timestamp.current[F]
      a <- attrs
      fm = RFileMeta(
        Ident.unsafe(id.id),
        now,
        a.contentType.contentType,
        ByteSize(a.length),
        a.sha256
      )
      _ <- saveMeta(fm)
    } yield ()

  def deleteAttr(id: BinaryId): F[Boolean] =
    RFileMeta.delete(Ident.unsafe(id.id)).transact(xa).map(_ > 0)

  def findAttr(id: BinaryId): OptionT[F, BinaryAttributes] =
    findMeta(id).map(fm =>
      BinaryAttributes(fm.checksum, SimpleContentType(fm.mimetype), fm.length.bytes)
    )

  def findMeta(id: BinaryId): OptionT[F, RFileMeta] =
    OptionT(RFileMeta.findById(Ident.unsafe(id.id)).transact(xa))

  def saveMeta(fm: RFileMeta): F[Unit] =
    upsert(RFileMeta.update(fm), fm)

  def updateCreated(id: BinaryId, created: Timestamp): F[Unit] =
    RFileMeta.updateCreated(Ident.unsafe(id.id), created).transact(xa).void

  /** No-op when `fm.checksum` is empty: the background worker yields empty attributes
    * when the binary has been deleted while its checksum job was still queued (see
    * `ComputeChecksum.computeSync`), and persisting that would resurrect the just-deleted
    * `filemeta` row as an orphan via the insert fallback below.
    */
  def updateChecksum(fm: RFileMeta): F[Unit] =
    if (fm.checksum.isEmpty) ().pure[F]
    else upsert(RFileMeta.updateChecksum(fm.id, fm.checksum), fm)

  /** Runs `doUpdate`; if it affects no row, falls back to inserting `row`.
    *
    * Two independent writers target the same `filemeta` row without coordination: the
    * foreground request (`saveMeta`) and the background checksum worker
    * (`updateChecksum`), see `sharry.store.FileStore`. Either can observe the row missing
    * and attempt this fallback insert concurrently, so one of them may hit a primary-key
    * violation.
    *
    * Only a primary-key violation (SQLState class "23", shared by H2/Postgres/MariaDB) is
    * treated as a possible race; any other insert failure (deadlock, connection loss,
    * ...) is raised immediately without retrying, since it isn't one.
    *
    * On a primary-key violation we retry `doUpdate` once, on the assumption that the
    * other writer's insert has landed by now. If the retry still affects no row, the row
    * may instead have been deleted concurrently by `Queries.deleteFile` (user delete or
    * the orphan-cleanup sweep, see `sharry.backend.share.OShare`) between our failed
    * insert and the retry — a legitimate delete, not an error. We tell the two apart with
    * an explicit lookup: if the row is truly gone, we swallow the original insert
    * failure; if it still exists, the retry's zero rows is unexplained, so we raise the
    * original insert failure.
    */
  private def upsert(doUpdate: ConnectionIO[Int], row: RFileMeta): F[Unit] = {
    val update = doUpdate.transact(xa)

    def isConstraintViolation(err: Throwable): Boolean = err match {
      case e: SQLException => Option(e.getSQLState).exists(_.startsWith("23"))
      case _               => false
    }

    def retryOrGiveUp(insertErr: Throwable): F[Unit] =
      logger.debug(
        s"Insert race on filemeta '${row.id.id}', retrying update"
      ) *> update.flatMap { n2 =>
        if (n2 > 0)
          logger.debug(s"Insert race on filemeta '${row.id.id}' resolved by update retry")
        else
          RFileMeta.findById(row.id).transact(xa).flatMap {
            case Some(_) => Sync[F].raiseError(insertErr)
            case None    =>
              logger.debug(
                s"filemeta '${row.id.id}' was deleted concurrently, ignoring insert race"
              )
          }
      }

    update.flatMap { n =>
      if (n > 0) ().pure[F]
      else
        RFileMeta.insert(row).transact(xa).attempt.flatMap {
          case Right(_)                                             => ().pure[F]
          case Left(insertErr) if !isConstraintViolation(insertErr) =>
            Sync[F].raiseError(insertErr)
          case Left(insertErr) => retryOrGiveUp(insertErr)
        }
    }
  }
}

object AttributeStore {

  def apply[F[_]: Sync](xa: Transactor[F]): AttributeStore[F] =
    new AttributeStore[F](xa)
}

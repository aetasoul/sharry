package sharry.store.doobie

import cats.data.OptionT
import cats.effect.*
import cats.implicits.*

import sharry.common.*
import sharry.store.records.RFileMeta

import binny.*
import doobie.*
import doobie.implicits.*

final private[store] class AttributeStore[F[_]: Sync](xa: Transactor[F]) {

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
    * violation. When the insert fails, we retry `doUpdate` once on the assumption that
    * the other writer's insert has landed by now and the row exists. If the retry still
    * affects no row, this wasn't a race after all, so the original insert failure is
    * raised instead of being silently swallowed.
    */
  private def upsert(doUpdate: ConnectionIO[Int], row: RFileMeta): F[Unit] = {
    val update = doUpdate.transact(xa)
    update.flatMap { n =>
      if (n > 0) ().pure[F]
      else
        RFileMeta.insert(row).transact(xa).attempt.flatMap {
          case Right(_)        => ().pure[F]
          case Left(insertErr) =>
            update.flatMap { n2 =>
              if (n2 > 0) ().pure[F] else Sync[F].raiseError(insertErr)
            }
        }
    }
  }
}

object AttributeStore {

  def apply[F[_]: Sync](xa: Transactor[F]): AttributeStore[F] =
    new AttributeStore[F](xa)
}

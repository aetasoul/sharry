package sharry.store

import cats.effect.*
import cats.syntax.all.*

import munit.*
import scodec.bits.ByteVector

import sharry.common.*
import sharry.store.records.RFileMeta

/** Regression test for issue #1835: the checksum computed by the background worker
  * (`FileStore.updateChecksum`) and the empty-checksum write made by the foreground
  * request (`FileStore.insertMeta`, via `excludeSha256`) target the same `filemeta` row
  * without coordination. Whichever one used to run last would silently keep/overwrite the
  * checksum, regardless of which value was correct.
  */
class ChecksumRaceTest extends CatsEffectSuite {

  private val realChecksum = ByteVector.fromValidHex("cafe")

  private def fastMeta(id: Ident, now: Timestamp): RFileMeta =
    RFileMeta(id, now, "application/octet-stream", ByteSize(100), ByteVector.empty)

  private def fullMeta(id: Ident, now: Timestamp): RFileMeta =
    RFileMeta(id, now, "application/octet-stream", ByteSize(100), realChecksum)

  test(
    "background checksum write is not lost when the foreground row does not exist yet"
  ) {
    StoreFixture.makeStore[IO].use { store =>
      for {
        id <- Ident.randomId[IO]
        now <- Timestamp.current[IO]
        // background worker races ahead of the foreground request: row doesn't exist yet
        _ <- store.fileStore.updateChecksum(fullMeta(id, now))
        // foreground's fast path (excludeSha256) creates the row afterwards
        _ <- store.fileStore.insertMeta(fastMeta(id, now))
        found <- store.fileStore.findMeta(id).value
      } yield assertEquals(found.map(_.checksum), Some(realChecksum))
    }
  }

  test("foreground fast-path write does not clobber an already-computed checksum") {
    StoreFixture.makeStore[IO].use { store =>
      for {
        id <- Ident.randomId[IO]
        now <- Timestamp.current[IO]
        // foreground creates the row first (e.g. TUS empty-file creation), no checksum yet
        _ <- store.fileStore.insertMeta(fastMeta(id, now))
        // background computes and persists the real checksum
        _ <- store.fileStore.updateChecksum(fullMeta(id, now))
        // foreground's finalize (e.g. TUS addChunk's Complete case) writes again
        _ <- store.fileStore.insertMeta(fastMeta(id, now).copy(mimetype = "text/plain"))
        found <- store.fileStore.findMeta(id).value
      } yield {
        assertEquals(found.map(_.checksum), Some(realChecksum))
        assertEquals(found.map(_.mimetype), Some("text/plain"))
      }
    }
  }

  test("empty-checksum write for a deleted file does not resurrect the filemeta row") {
    StoreFixture.makeStore[IO].use { store =>
      for {
        id <- Ident.randomId[IO]
        now <- Timestamp.current[IO]
        // file was deleted while its checksum job was queued: the worker then computes
        // empty attributes (binary gone) and tries to persist them on a missing row
        _ <- store.fileStore.updateChecksum(fastMeta(id, now))
        found <- store.fileStore.findMeta(id).value
      } yield assertEquals(found, None)
    }
  }

  test(
    "concurrent insert race (both rows missing at once) does not fail and never loses the checksum"
  ) {
    StoreFixture.makeStore[IO].use { store =>
      // Run several fresh ids in parallel: neither insertMeta nor updateChecksum can
      // assume it "goes first" here, so on at least some of these both writers may
      // observe the row missing and race to insert it concurrently.
      List.range(0, 20).parTraverse { _ =>
        for {
          id <- Ident.randomId[IO]
          now <- Timestamp.current[IO]
          _ <- store.fileStore
            .insertMeta(fastMeta(id, now))
            .both(store.fileStore.updateChecksum(fullMeta(id, now)))
          found <- store.fileStore.findMeta(id).value
        } yield assertEquals(found.map(_.checksum), Some(realChecksum))
      }
    }
  }

  test(
    "insert race resolved by a concurrent legitimate delete never raises"
  ) {
    // Same insert race as above (both writers observe the row missing), but a third actor
    // deletes the row directly, the same way `Queries.deleteFile` does (user delete or the
    // orphan-cleanup sweep, see `bug_upsert_retry_once` note). Whichever writer loses the
    // insert race must not blow up when its update-retry then finds the row gone: that's a
    // legitimate delete, not an unresolved race - `parTupled` already fails this test if any
    // of the three actors raises, which is exactly that guarantee.
    //
    // The row's final state (present or absent, and with which checksum) is deliberately
    // NOT asserted here: depending on exactly where the delete lands relative to the two
    // writers, more than one outcome is legitimate - including one writer's fallback insert
    // resurrecting the row after the delete (see `bug_orphan_delete_race`, a separate,
    // already-tracked gap in `saveMeta`/`insertMeta`, not something this test's point is
    // about).
    StoreFixture.makeStore[IO].use { store =>
      List.range(0, 20).parTraverse { _ =>
        for {
          id <- Ident.randomId[IO]
          now <- Timestamp.current[IO]
          _ <- (
            store.fileStore.insertMeta(fastMeta(id, now)),
            store.fileStore.updateChecksum(fullMeta(id, now)),
            store.transact(RFileMeta.delete(id))
          ).parTupled
        } yield ()
      }
    }
  }
}

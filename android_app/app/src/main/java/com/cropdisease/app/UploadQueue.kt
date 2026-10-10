package com.cropdisease.app

import android.content.ContentValues
import android.content.Context
import android.content.Intent
import android.database.Cursor
import android.database.sqlite.SQLiteDatabase
import android.database.sqlite.SQLiteOpenHelper
import android.net.Uri
import java.io.File

/**
 * Photos waiting to be uploaded to Google Drive, saved on the phone
 * (a small SQLite database in app-private storage), so an upload of
 * thousands of photos survives the app being closed, the phone
 * restarting or the connection dropping. UploadWorker works through it
 * in the background; the upload screen only adds to it and shows progress.
 *
 * Every row gets its image_id when it is queued and keeps it on every
 * retry — DriveUploader checks Drive for <image_id>.jpg before re-sending,
 * so a photo is never put on Drive (and through Spark) twice.
 *
 * The photo itself is NOT copied when the app can keep read access to it
 * (persisted uri permission, or a folder chosen with "Choose folder"); only
 * camera photos and the rare gallery photo whose access can't be kept are
 * stored as files in filesDir/upload_queue/ — deleted once uploaded.
 *
 * Rows are per Google account (user_email): the worker only ever uploads
 * the signed-in user's own rows, with their own token, into their own folder.
 */
class UploadQueue private constructor(private val context: Context) :
    SQLiteOpenHelper(context, "upload_queue.db", null, 1) {

    companion object {
        const val QUEUED = "QUEUED"
        const val UPLOADED = "UPLOADED"
        const val FAILED = "FAILED"       // can be retried ("Retry failed")
        const val REJECTED = "REJECTED"   // not a usable photo — retrying won't help

        @Volatile private var instance: UploadQueue? = null

        fun get(context: Context): UploadQueue = instance ?: synchronized(this) {
            instance ?: UploadQueue(context.applicationContext).also { instance = it }
        }

        /** Where queued copies (camera photos, uncopyable gallery photos) live. */
        fun ownedDir(context: Context) = File(context.filesDir, "upload_queue")
    }

    data class Item(
        val imageId: String,
        val userEmail: String,
        val uri: String,
        val displayName: String,
        val mimeType: String,
        val location: String?,
        val gpsLat: Double? = null,
        val gpsLon: Double? = null,
        val gpsAccM: Float? = null,
        /** The uri is our own copy in ownedDir — delete after upload. */
        val ownedFile: Boolean = false,
        /** Folder ("Choose folder") whose access this photo is read through. */
        val treeUri: String? = null,
        val state: String = QUEUED,
        val error: String? = null,
        val driveFileId: String? = null,
    )

    data class Counts(val total: Int, val queued: Int, val uploaded: Int, val failed: Int, val rejected: Int) {
        val done get() = total - queued
    }

    override fun onCreate(db: SQLiteDatabase) {
        db.execSQL(
            """CREATE TABLE queue (
                image_id TEXT PRIMARY KEY,
                user_email TEXT NOT NULL,
                uri TEXT NOT NULL,
                display_name TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                location TEXT,
                gps_lat REAL, gps_lon REAL, gps_acc REAL,
                owned_file INTEGER NOT NULL DEFAULT 0,
                tree_uri TEXT,
                state TEXT NOT NULL,
                error TEXT,
                drive_file_id TEXT
            )"""
        )
        db.execSQL("CREATE INDEX queue_user_state ON queue(user_email, state)")
    }

    override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {}

    /**
     * Adds photos to the queue. If nothing is waiting for this user, the
     * previous (finished) upload's rows are cleared first, so the progress
     * shown is "X of N" for the photos queued since the queue was last empty.
     */
    fun enqueue(items: List<Item>) {
        if (items.isEmpty()) return
        val email = items.first().userEmail
        val db = writableDatabase
        if (!hasQueued(email)) clearFinished(email)
        db.beginTransaction()
        try {
            for (item in items) {
                db.insertWithOnConflict("queue", null, ContentValues().apply {
                    put("image_id", item.imageId)
                    put("user_email", item.userEmail)
                    put("uri", item.uri)
                    put("display_name", item.displayName)
                    put("mime_type", item.mimeType)
                    put("location", item.location)
                    item.gpsLat?.let { put("gps_lat", it) }
                    item.gpsLon?.let { put("gps_lon", it) }
                    item.gpsAccM?.let { put("gps_acc", it) }
                    put("owned_file", if (item.ownedFile) 1 else 0)
                    put("tree_uri", item.treeUri)
                    put("state", QUEUED)
                }, SQLiteDatabase.CONFLICT_IGNORE)
            }
            db.setTransactionSuccessful()
        } finally {
            db.endTransaction()
        }
    }

    fun nextQueued(email: String, limit: Int): List<Item> = query(
        "user_email = ? AND state = ?", arrayOf(email, QUEUED), limit
    )

    fun hasQueued(email: String): Boolean = readableDatabase.rawQuery(
        "SELECT 1 FROM queue WHERE user_email = ? AND state = ? LIMIT 1", arrayOf(email, QUEUED)
    ).use { it.moveToFirst() }

    /** True while a queued (or retryable) photo is still read through this folder. */
    fun usesTree(treeUri: String): Boolean = readableDatabase.rawQuery(
        "SELECT 1 FROM queue WHERE tree_uri = ? AND state IN (?, ?) LIMIT 1", arrayOf(treeUri, QUEUED, FAILED)
    ).use { it.moveToFirst() }

    fun counts(email: String): Counts {
        val byState = mutableMapOf<String, Int>()
        readableDatabase.rawQuery(
            "SELECT state, COUNT(*) FROM queue WHERE user_email = ? GROUP BY state", arrayOf(email)
        ).use { c -> while (c.moveToNext()) byState[c.getString(0)] = c.getInt(1) }
        return Counts(
            total = byState.values.sum(),
            queued = byState[QUEUED] ?: 0,
            uploaded = byState[UPLOADED] ?: 0,
            failed = byState[FAILED] ?: 0,
            rejected = byState[REJECTED] ?: 0,
        )
    }

    /** Current rows for these image_ids (used for the inline result rows). */
    fun items(imageIds: Collection<String>): Map<String, Item> {
        if (imageIds.isEmpty()) return emptyMap()
        val out = mutableMapOf<String, Item>()
        for (chunk in imageIds.chunked(500)) {
            val marks = chunk.joinToString(",") { "?" }
            for (item in query("image_id IN ($marks)", chunk.toTypedArray(), null)) out[item.imageId] = item
        }
        return out
    }

    fun markUploaded(item: Item, driveFileId: String) {
        update(item.imageId, UPLOADED, null, driveFileId)
        releaseSource(item)
    }

    fun markRejected(item: Item, error: String) {
        update(item.imageId, REJECTED, error, null)
        releaseSource(item)
    }

    /** Keeps the photo's source, so "Retry failed" can send it again. */
    fun markFailed(item: Item, error: String) = update(item.imageId, FAILED, error, null)

    /** Puts every FAILED photo of this user back in the queue. Returns how many. */
    fun retryFailed(email: String): Int = writableDatabase.update(
        "queue", ContentValues().apply { put("state", QUEUED); putNull("error") },
        "user_email = ? AND state = ?", arrayOf(email, FAILED)
    )

    /** Drops finished rows (releasing what FAILED rows still held on to). */
    private fun clearFinished(email: String) {
        val failed = query("user_email = ? AND state = ?", arrayOf(email, FAILED), null)
        writableDatabase.delete("queue", "user_email = ? AND state != ?", arrayOf(email, QUEUED))
        for (item in failed) releaseSource(item)  // after the delete, so their folders count as unused
    }

    /** Lets go of the photo once it is no longer needed: our own copy is
     * deleted, a persisted read permission is released (Android allows an
     * app only a limited number), and a chosen folder's permission is
     * released when no queued photo is read through it any more. */
    private fun releaseSource(item: Item) {
        val uri = Uri.parse(item.uri)
        try {
            when {
                item.ownedFile -> uri.path?.let { File(it).delete() }
                item.treeUri != null -> {
                    if (!usesTree(item.treeUri)) context.contentResolver.releasePersistableUriPermission(
                        Uri.parse(item.treeUri), Intent.FLAG_GRANT_READ_URI_PERMISSION
                    )
                }
                else -> context.contentResolver.releasePersistableUriPermission(
                    uri, Intent.FLAG_GRANT_READ_URI_PERMISSION
                )
            }
        } catch (e: Exception) {
            // nothing was held (e.g. permission never persisted) — fine
        }
    }

    private fun update(imageId: String, state: String, error: String?, driveFileId: String?) {
        writableDatabase.update("queue", ContentValues().apply {
            put("state", state)
            put("error", error?.take(300))
            if (driveFileId != null) put("drive_file_id", driveFileId)
        }, "image_id = ?", arrayOf(imageId))
    }

    private fun query(where: String, args: Array<String>, limit: Int?): List<Item> =
        readableDatabase.query(
            "queue", null, where, args, null, null, "rowid", limit?.toString()
        ).use { c -> buildList { while (c.moveToNext()) add(c.toItem()) } }

    private fun Cursor.toItem(): Item {
        fun str(col: String) = getColumnIndexOrThrow(col).let { if (isNull(it)) null else getString(it) }
        fun dbl(col: String) = getColumnIndexOrThrow(col).let { if (isNull(it)) null else getDouble(it) }
        return Item(
            imageId = str("image_id")!!,
            userEmail = str("user_email")!!,
            uri = str("uri")!!,
            displayName = str("display_name")!!,
            mimeType = str("mime_type")!!,
            location = str("location"),
            gpsLat = dbl("gps_lat"),
            gpsLon = dbl("gps_lon"),
            gpsAccM = dbl("gps_acc")?.toFloat(),
            ownedFile = getInt(getColumnIndexOrThrow("owned_file")) == 1,
            treeUri = str("tree_uri"),
            state = str("state")!!,
            error = str("error"),
            driveFileId = str("drive_file_id"),
        )
    }
}

package com.cropdisease.app

import android.content.Context
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.util.TimeZone

/**
 * The signed-in user's upload history, saved on the phone (app-private
 * storage: other apps can't read it, and it is removed when the app is
 * uninstalled). One file per Google account, so two farmers sharing a
 * phone never see each other's history.
 *
 * Items have exactly the shape DriveRest.myHistory returns, so Home and
 * History render saved and fresh items with the same code:
 *   {image_id, drive_file_id, processed_file_id, location, uploaded_at,
 *    outcome, prediction | null, error_message | null}
 *
 * Google Drive stays the source of truth — this is a copy that lets the
 * screens open instantly and work offline, and that keeps photos older
 * than the newest 200 Drive returns per load. Written by:
 *   - MainActivity, when a photo is uploaded (pending) and when its result arrives
 *   - Home/History, merging every list they read from Drive
 * and emptied by applyFactoryReset() after the laptop's factory reset.
 */
class HistoryStore(context: Context, email: String) {

    companion object {
        // ~0.5 KB per item: even this many is only a few MB.
        private const val MAX_ITEMS = 20_000
        private val lock = Any()

        fun nowIso(): String = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US)
            .apply { timeZone = TimeZone.getTimeZone("UTC") }
            .format(Date())

        private fun sha1(text: String): String =
            MessageDigest.getInstance("SHA-1").digest(text.toByteArray())
                .joinToString("") { "%02x".format(it) }

        /** A result is final once Spark has written COMPLETED/FAILED; never
         * let a later "pending" (e.g. from a partial Drive listing) undo it. */
        private fun isFinal(item: JSONObject) = item.optString("outcome") != "pending"
    }

    private val file = File(File(context.filesDir, "history"), "${sha1(email.lowercase())}.json")
    private val syncFile = File(file.parentFile, "${file.nameWithoutExtension}.results_since")
    private val resetFile = File(file.parentFile, "${file.nameWithoutExtension}.reset_seen")

    /** Drops the saved copy if the laptop's factory reset (with "delete
     * Drive files") ran since this phone last looked: otherwise the old
     * results stay here forever, since merge() never removes anything.
     * Call before reading Drive, which then refills it. Returns true if the
     * saved history was cleared. Photos still in the UploadQueue are kept. */
    fun applyFactoryReset(client: okhttp3.OkHttpClient, token: String, folders: DriveRest.Folders): Boolean {
        val resetAt = DriveRest.resetMarker(client, token, folders.resultsId) ?: return false
        val seen = try { resetFile.readText().trim() } catch (e: Exception) { "" }
        if (resetAt == seen) return false
        synchronized(lock) {
            file.delete()
            syncFile.delete()  // read every result again, not only the newer ones
            try {
                resetFile.parentFile?.mkdirs()
                resetFile.writeText(resetAt)
            } catch (e: Exception) { }
        }
        return true
    }

    /** Drive modifiedTime up to which /results has been read into this
     * store (DriveRest.resultsSince), or null before the first full read. */
    var resultsSyncedUntil: String?
        get() = try { syncFile.readText().trim().ifEmpty { null } } catch (e: Exception) { null }
        set(value) {
            try {
                syncFile.parentFile?.mkdirs()
                if (value == null) syncFile.delete() else syncFile.writeText(value)
            } catch (e: Exception) { }
        }

    /** Brings every result written since the last call into the store.
     * Re-reads the last minute again, since results written at the same
     * moment could otherwise be skipped; merging them twice is harmless. */
    fun syncResults(client: okhttp3.OkHttpClient, token: String, folders: DriveRest.Folders) {
        val since = resultsSyncedUntil?.let { minusOneMinute(it) }
        val (items, newest) = DriveRest.resultsSince(client, token, folders, since)
        merge(items)
        if (newest != null) resultsSyncedUntil = newest
    }

    private fun minusOneMinute(iso: String): String? = try {
        val f = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US).apply { timeZone = TimeZone.getTimeZone("UTC") }
        f.format(Date(f.parse(iso.take(19))!!.time - 60_000))
    } catch (e: Exception) {
        null  // unparseable: read everything again
    }

    /** Saved items, newest first. Empty if nothing has been saved yet. */
    fun load(): List<JSONObject> = synchronized(lock) { readMap().values.sortedByDescending { it.optString("uploaded_at") } }

    /** Adds or updates items by image_id (Drive's view wins, except it
     * never turns a finished result back into pending). */
    fun merge(items: Collection<JSONObject>) = synchronized(lock) {
        if (items.isEmpty()) return@synchronized
        val map = readMap()
        for (item in items) {
            val id = item.optString("image_id")
            if (id.isEmpty()) continue
            val old = map[id]
            map[id] = if (old != null && isFinal(old) && !isFinal(item)) old else item
        }
        write(map)
    }

    /** A just-uploaded photo, before Spark has a result. Big uploads should
     * merge() many of these at once: every save rewrites the whole file. */
    fun pendingItem(imageId: String, driveFileId: String?, location: String?): JSONObject =
        JSONObject()
            .put("image_id", imageId)
            .put("drive_file_id", driveFileId.orEmpty())
            .put("processed_file_id", "")
            .put("location", location ?: JSONObject.NULL)
            .put("uploaded_at", nowIso())
            .put("outcome", "pending")
            .put("prediction", JSONObject.NULL)
            .put("error_message", JSONObject.NULL)

    /** Applies a Spark result file (/results/<image_id>.json, as MainActivity
     * polls it) to the saved item for that photo. */
    fun applyResult(imageId: String, result: JSONObject) = synchronized(lock) {
        val map = readMap()
        val item = map[imageId] ?: JSONObject().put("image_id", imageId).put("drive_file_id", "")
            .put("location", JSONObject.NULL).put("uploaded_at", nowIso())
        when (result.optString("status")) {
            "COMPLETED" -> {
                val p = result.optJSONObject("prediction") ?: JSONObject()
                val cls = p.optString("predicted_class", "unknown")
                item.put("outcome", if (cls.equals("healthy", ignoreCase = true)) "healthy" else "diseased")
                    .put("prediction", JSONObject()
                        .put("predicted_class", cls)
                        .put("prediction_confidence", p.optDouble("prediction_confidence", 0.0))
                        .put("risk_level", p.optString("risk_level", "UNKNOWN")))
                    .put("error_message", JSONObject.NULL)
            }
            "FAILED" -> item.put("outcome", "failed").put("prediction", JSONObject.NULL)
                .put("error_message", result.optString("error_message").ifEmpty { JSONObject.NULL })
            else -> return@synchronized
        }
        if (!result.isNull("processed_file_id")) {
            item.put("processed_file_id", result.optString("processed_file_id"))
        }
        map[imageId] = item
        write(map)
    }

    private fun readMap(): LinkedHashMap<String, JSONObject> {
        val map = LinkedHashMap<String, JSONObject>()
        if (!file.exists()) return map
        try {
            val array = JSONArray(file.readText())
            for (i in 0 until array.length()) {
                val item = array.getJSONObject(i)
                map[item.optString("image_id")] = item
            }
        } catch (e: Exception) {
            // corrupt/half-written file: start again, Drive refills it
        }
        return map
    }

    private fun write(map: Map<String, JSONObject>) {
        val items = map.values.sortedByDescending { it.optString("uploaded_at") }.take(MAX_ITEMS)
        try {
            file.parentFile?.mkdirs()
            // Write-then-rename, so a crash mid-write never leaves a broken file.
            val tmp = File(file.parentFile, file.name + ".tmp")
            tmp.writeText(JSONArray(items).toString())
            if (!tmp.renameTo(file)) {
                file.delete()
                tmp.renameTo(file)
            }
        } catch (e: Exception) {
            // Phone storage full etc.: the saved copy just isn't updated —
            // never fail an upload over it. Drive still has everything.
        }
    }
}

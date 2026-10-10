package com.cropdisease.app

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.widget.ImageView
import okhttp3.OkHttpClient
import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * Loads a photo from Google Drive — the processed (marked-up) image from
 * the farmer's /results/<email>/ folder, or the original from
 * /images/<email>/ while no result exists yet — downsampled while
 * decoding (thumbnail size by default, larger for the side-by-side view).
 *
 * Asks Drive for its own small preview first (thumbnailLink: a few KB),
 * and only downloads the whole file when Drive has no preview yet (a
 * file uploaded seconds ago) or the preview request fails. A History page
 * of 50 rows used to download 50 full photos.
 *
 * Each thumbnail is saved on the phone after its first download (see
 * cacheFile). No view-recycling guard: every screen that uses this builds
 * its rows once into a plain LinearLayout (not a RecyclerView), so there's
 * no risk of a slow response landing on a reused/wrong row.
 */
object ThumbnailLoader {
    private const val THUMB_PX = 200
    // A few at a time: enough to fill a screen quickly without starving the
    // phone's connection while a batch is uploading.
    private val pool = Executors.newFixedThreadPool(4)

    // Thumbnails saved on the phone (cacheDir/thumbs), so History/Home show
    // them instantly and offline. Display copies only — never uploaded or
    // analysed. Android may clear cacheDir when storage is low; they are
    // simply downloaded again.
    private const val DISK_LIMIT_BYTES = 50L * 1024 * 1024
    private val writes = AtomicInteger(0)

    private fun cacheFile(target: ImageView, driveFileId: String, maxPx: Int) =
        File(File(target.context.cacheDir, "thumbs"), "${driveFileId}_$maxPx.jpg")

    /** Oldest-first delete once the folder passes DISK_LIMIT_BYTES. */
    private fun trim(dir: File) {
        val files = dir.listFiles()?.sortedBy { it.lastModified() } ?: return
        var total = files.sumOf { it.length() }
        for (f in files) {
            if (total <= DISK_LIMIT_BYTES) break
            total -= f.length()
            f.delete()
        }
    }

    fun load(client: OkHttpClient, token: String?, driveFileId: String, target: ImageView, maxPx: Int = THUMB_PX) {
        if (driveFileId.isEmpty()) return
        val saved = cacheFile(target, driveFileId, maxPx)
        pool.execute {
            if (saved.exists()) {
                BitmapFactory.decodeFile(saved.path)?.let { bitmap ->
                    target.post { target.setImageBitmap(bitmap) }
                    return@execute
                }
            }
            if (token == null) return@execute  // offline / signed out: leave the placeholder
            val bytes = try {
                DriveRest.downloadThumbnail(client, token, driveFileId, maxPx)
            } catch (e: Exception) {
                null
            } ?: try {
                DriveRest.downloadBytes(client, token, driveFileId)
            } catch (e: Exception) {
                return@execute  // leave the placeholder
            }
            val bounds = BitmapFactory.Options().apply { inJustDecodeBounds = true }
            BitmapFactory.decodeByteArray(bytes, 0, bytes.size, bounds)
            var sample = 1
            while (bounds.outWidth / sample > maxPx * 2 || bounds.outHeight / sample > maxPx * 2) {
                sample *= 2
            }
            val opts = BitmapFactory.Options().apply { inSampleSize = sample }
            val bitmap = BitmapFactory.decodeByteArray(bytes, 0, bytes.size, opts) ?: return@execute
            target.post { target.setImageBitmap(bitmap) }
            try {
                saved.parentFile?.mkdirs()
                saved.outputStream().use { bitmap.compress(Bitmap.CompressFormat.JPEG, 85, it) }
                if (writes.incrementAndGet() % 50 == 0) saved.parentFile?.let { trim(it) }
            } catch (e: Exception) { /* not saved: downloaded again next time */ }
        }
    }
}

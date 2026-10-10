package com.cropdisease.app

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.provider.MediaStore
import androidx.core.content.ContextCompat
import okhttp3.OkHttpClient
import java.io.IOException
import java.util.Locale

/**
 * Sends ONE queued photo to the farmer's own Google Drive folder
 * /images/<email>/<image_id>.jpg, tagged with the appProperties the
 * laptop's Drive watcher reads (image_id, original_filename, location,
 * gps_*). Used by UploadWorker.
 *
 * The photo's bytes are sent exactly as they are on the phone — never
 * resized or re-encoded — so Spark and the CNN analyse the original.
 *
 * Blocking: call from a background thread.
 */
object DriveUploader {
    // Same per-photo limit the laptop's Drive watcher enforces (MAX_IMAGE_SIZE).
    const val MAX_PHOTO_BYTES = 15 * 1024 * 1024
    // Tries per photo on a dropped connection / Drive rate limit / 5xx,
    // waiting 1s, 2s, 4s (+ jitter) in between.
    private const val ATTEMPTS = 4
    private const val RETRY_BASE_DELAY_MS = 1000L

    sealed class Outcome {
        data class Uploaded(val driveFileId: String) : Outcome()
        /** Not a usable photo (wrong type, unreadable, too big): don't retry. */
        data class Rejected(val error: String) : Outcome()
        /** Drive refused it (e.g. no access to the folder): "Retry failed" may help later. */
        data class Failed(val error: String) : Outcome()
        /** Network/Drive trouble that outlasted the retries: leave it queued. */
        data class Transient(val error: String) : Outcome()
        /** Google sign-in is needed again: leave it queued. */
        object SignInNeeded : Outcome()
    }

    fun upload(
        context: Context, uploadClient: OkHttpClient, apiClient: OkHttpClient,
        item: UploadQueue.Item, folders: DriveRest.Folders,
    ): Outcome {
        val invalid = context.getString(R.string.error_invalid_file)
        if (item.mimeType != "image/jpeg" && item.mimeType != "image/png") return Outcome.Rejected(invalid)
        val bytes = readPhotoBytes(context, Uri.parse(item.uri)) ?: return Outcome.Rejected(invalid)
        if (bytes.isEmpty() || bytes.size > MAX_PHOTO_BYTES) return Outcome.Rejected(invalid)

        val properties = mutableMapOf(
            "image_id" to item.imageId,
            "original_filename" to item.displayName.take(80),
        )
        item.location?.let { properties["location"] = it.take(100) }
        if (item.gpsLat != null && item.gpsLon != null) {
            properties["gps_lat"] = String.format(Locale.US, "%.6f", item.gpsLat)
            properties["gps_lon"] = String.format(Locale.US, "%.6f", item.gpsLon)
            item.gpsAccM?.let { properties["gps_acc_m"] = String.format(Locale.US, "%.0f", it) }
        }

        var lastError: Exception? = null
        for (attempt in 1..ATTEMPTS) {
            // Fresh token per attempt: Google caches it, and a long upload
            // outlives the ~1 hour a single token is valid for.
            val token = DriveAuth.accessTokenBlocking(context) ?: return Outcome.SignInNeeded
            try {
                // A retry first checks whether the previous attempt actually
                // reached Drive (only the response was lost).
                val driveFileId = (if (attempt > 1) DriveRest.findImage(apiClient, token, folders.imagesId, item.imageId) else null)
                    ?: DriveRest.uploadImage(uploadClient, token, folders.imagesId, bytes, item.mimeType, item.imageId, properties)
                return Outcome.Uploaded(driveFileId)
            } catch (e: DriveRest.DriveHttpException) {
                when {
                    e.code == 401 && attempt < ATTEMPTS -> Unit  // token refreshed next attempt
                    e.code == 401 -> return Outcome.SignInNeeded
                    DriveRest.isRetryable(e) -> Unit
                    e.code == 403 || e.code == 404 -> return Outcome.Failed(context.getString(R.string.error_drive_no_access))
                    else -> return Outcome.Failed(e.message ?: context.getString(R.string.error_unknown_error))
                }
                lastError = e
            } catch (e: IOException) {
                lastError = e   // dropped connection / timeout: retry
            } catch (e: Exception) {
                return Outcome.Failed(e.message ?: context.getString(R.string.error_unknown_error))
            }
            if (attempt < ATTEMPTS) {
                Thread.sleep(RETRY_BASE_DELAY_MS * (1L shl (attempt - 1)) + (0..500).random())
            }
        }
        return Outcome.Transient(lastError?.message ?: context.getString(R.string.error_unknown_error))
    }

    /** The photo's bytes WITH its GPS EXIF tags where Android allows it:
     * setRequireOriginal needs ACCESS_MEDIA_LOCATION and a MediaStore uri
     * (gallery documents are converted with getMediaUri). Anything else
     * falls back to the plain uri, whose GPS tags Android may have removed. */
    fun readPhotoBytes(context: Context, uri: Uri): ByteArray? {
        val mediaLocation = ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_MEDIA_LOCATION) ==
            PackageManager.PERMISSION_GRANTED
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && mediaLocation && uri.scheme == "content") {
            try {
                val media = if (uri.authority == MediaStore.AUTHORITY) uri else MediaStore.getMediaUri(context, uri)
                if (media != null) {
                    context.contentResolver.openInputStream(MediaStore.setRequireOriginal(media))
                        ?.use { return it.readBytes() }
                }
            } catch (e: Exception) {
                // not a media-store photo (e.g. Drive, Downloads provider) -> plain read below
            }
        }
        return try {
            context.contentResolver.openInputStream(uri)?.use { it.readBytes() }
        } catch (e: Exception) {
            null
        }
    }
}

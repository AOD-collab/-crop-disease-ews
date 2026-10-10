package com.cropdisease.app

import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException

/**
 * Everything the app does with Google Drive (REST API v3,
 * https://developers.google.com/drive/api/reference/rest/v3), with the
 * signed-in farmer's own OAuth access token (DriveAuth). Replaces the calls
 * the app used to make to our REST API.
 *
 *   myFolders()    this farmer's two PRIVATE folders, created and shared
 *                  only with them by scripts/drive_share_user.py:
 *                    /images/<email>/   (writer: the app uploads here)
 *                    /results/<email>/  (reader: Spark writes results here)
 *   uploadImage()  Android -> Drive /images/<email>/<image_id>.jpg
 *   findResults()  Drive /results/<email>/<image_id>.json written by Spark
 *   resetMarker()  when the laptop last factory-reset the Drive files
 *   myHistory()    the user's uploads + results, in the same JSON shape the
 *                  old GET /my/history returned (plus processed_file_id),
 *                  so Home/History render them with their existing code.
 *
 * All functions are BLOCKING — call them from a background thread.
 */
object DriveRest {
    private const val DRIVE = "https://www.googleapis.com/drive/v3"
    private const val UPLOAD = "https://www.googleapis.com/upload/drive/v3"
    private val JSON = "application/json; charset=utf-8".toMediaType()

    /** HTTP error from Google Drive; code 401 = sign in again, 403/404 = no access to the folder. */
    class DriveHttpException(val code: Int, message: String) : IOException(message)

    data class Me(val email: String, val displayName: String?)

    /** The signed-in farmer's private folders. */
    data class Folders(val imagesId: String, val resultsId: String)

    private const val FOLDER_MIME = "application/vnd.google-apps.folder"
    private const val RESET_MARKER_NAME = "_reset_marker.json"

    private fun authed(token: String) = Request.Builder().header("Authorization", "Bearer $token")

    private fun execute(client: OkHttpClient, request: Request): okhttp3.Response {
        val response = client.newCall(request).execute()
        if (!response.isSuccessful) {
            val body = response.body?.string()?.take(300)
            response.close()
            throw DriveHttpException(response.code, "Google Drive error ${response.code}: $body")
        }
        return response
    }

    private fun getJson(client: OkHttpClient, token: String, url: String): JSONObject =
        execute(client, authed(token).url(url).get().build()).use { JSONObject(it.body!!.string()) }

    private fun q(value: String) = value.replace("\\", "\\\\").replace("'", "\\'")

    /** The signed-in Google account (used as the user's identity in the app). */
    fun aboutMe(client: OkHttpClient, token: String): Me {
        val user = getJson(client, token, "$DRIVE/about?fields=user(emailAddress,displayName)")
            .getJSONObject("user")
        return Me(user.getString("emailAddress").lowercase(), user.optString("displayName").ifEmpty { null })
    }

    /**
     * Finds this farmer's two private folders among the folders shared with
     * them. Both are named after their email; the one they may add files to
     * is /images/<email>/, the read-only one is /results/<email>/.
     * Returns null if the administrator hasn't shared them yet.
     */
    fun myFolders(client: OkHttpClient, token: String, email: String): Folders? {
        val url = "$DRIVE/files".toHttpUrl().newBuilder()
            .addQueryParameter(
                "q",
                "name = '${q(email)}' and mimeType = '$FOLDER_MIME' and trashed = false and sharedWithMe"
            )
            .addQueryParameter("fields", "files(id,capabilities(canAddChildren),appProperties)")
            .build().toString()
        val files = getJson(client, token, url).optJSONArray("files") ?: return null
        var images: String? = null
        var results: String? = null
        for (i in 0 until files.length()) {
            val f = files.getJSONObject(i)
            val kind = f.optJSONObject("appProperties")?.optString("kind").orEmpty()
            val canUpload = f.optJSONObject("capabilities")?.optBoolean("canAddChildren") == true
            when {
                kind == "user_images" || (kind.isEmpty() && canUpload) -> images = f.getString("id")
                kind == "user_results" || (kind.isEmpty() && !canUpload) -> results = f.getString("id")
            }
        }
        return if (images != null && results != null) Folders(images, results) else null
    }

    /**
     * Uploads one photo to the farmer's own /images/<email>/ folder as <imageId>.jpg/.png
     * (resumable upload: works for photos larger than 5 MB). `properties`
     * become Drive appProperties (image_id, location, ...), which the
     * laptop's Drive watcher reads. Returns the Drive file id.
     */
    fun uploadImage(
        client: OkHttpClient,
        token: String,
        imagesFolderId: String,
        bytes: ByteArray,
        mimeType: String,
        imageId: String,
        properties: Map<String, String>,
    ): String {
        val extension = if (mimeType == "image/png") ".png" else ".jpg"
        val metadata = JSONObject()
            .put("name", "$imageId$extension")
            .put("mimeType", mimeType)
            .put("parents", JSONArray().put(imagesFolderId))
            .put("appProperties", JSONObject(properties.filterValues { it.isNotEmpty() }))

        // 1. start a resumable upload session
        val sessionUrl = execute(
            client,
            authed(token)
                .url("$UPLOAD/files?uploadType=resumable&fields=id")
                .header("X-Upload-Content-Type", mimeType)
                .header("X-Upload-Content-Length", bytes.size.toString())
                .post(metadata.toString().toRequestBody(JSON))
                .build()
        ).use { it.header("Location") } ?: throw IOException("Google Drive did not return an upload URL")

        // 2. send the bytes
        return execute(
            client,
            authed(token).url(sessionUrl).put(bytes.toRequestBody(mimeType.toMediaTypeOrNull())).build()
        ).use { JSONObject(it.body!!.string()).getString("id") }
    }

    /** Drive file id of <imageId>.jpg/.png in the farmer's /images folder, or
     * null. Used before retrying an upload whose response was lost, so a
     * retry never puts the same photo on Drive (and through Spark) twice. */
    fun findImage(client: OkHttpClient, token: String, imagesFolderId: String, imageId: String): String? {
        val url = "$DRIVE/files".toHttpUrl().newBuilder()
            .addQueryParameter(
                "q",
                "'$imagesFolderId' in parents and trashed = false and " +
                    "(name = '${q(imageId)}.jpg' or name = '${q(imageId)}.png')"
            )
            .addQueryParameter("fields", "files(id)")
            .addQueryParameter("pageSize", "1")
            .build().toString()
        val files = getJson(client, token, url).optJSONArray("files") ?: return null
        return if (files.length() > 0) files.getJSONObject(0).getString("id") else null
    }

    /** True for Drive errors worth retrying: rate limits (429, or 403 with a
     * rateLimitExceeded reason) and server-side 5xx. */
    fun isRetryable(e: DriveHttpException): Boolean =
        e.code == 429 || e.code >= 500 ||
            (e.code == 403 && e.message.orEmpty().contains("rateLimitExceeded", ignoreCase = true))

    /**
     * Which of `imageIds` already have a result file in the farmer's
     * /results/<email>/ folder. ONE Drive request per 20 ids, however many
     * photos are waiting. Returns image_id -> Drive file id of <image_id>.json.
     */
    fun findResults(
        client: OkHttpClient, token: String, resultsFolderId: String, imageIds: Collection<String>,
    ): Map<String, String> {
        val found = mutableMapOf<String, String>()
        for (chunk in imageIds.chunked(20)) {
            val names = chunk.joinToString(" or ") { "name = '${q(it)}.json'" }
            val url = "$DRIVE/files".toHttpUrl().newBuilder()
                .addQueryParameter("q", "'$resultsFolderId' in parents and trashed = false and ($names)")
                .addQueryParameter("fields", "files(id,name)")
                .addQueryParameter("pageSize", "100")
                .build().toString()
            val files = getJson(client, token, url).optJSONArray("files") ?: JSONArray()
            for (i in 0 until files.length()) {
                val f = files.getJSONObject(i)
                found[f.getString("name").removeSuffix(".json")] = f.getString("id")
            }
        }
        return found
    }

    /** reset_at of the newest reset marker the laptop's factory reset left in
     * /results/<email>/ (storage/reset.py RESET_MARKER_NAME), or null if
     * the system was never reset with "delete Drive files" ticked. */
    fun resetMarker(client: OkHttpClient, token: String, resultsFolderId: String): String? {
        val url = "$DRIVE/files".toHttpUrl().newBuilder()
            .addQueryParameter("q", "'$resultsFolderId' in parents and trashed = false and name = '$RESET_MARKER_NAME'")
            .addQueryParameter("orderBy", "modifiedTime desc")
            .addQueryParameter("pageSize", "1")
            .addQueryParameter("fields", "files(appProperties)")
            .build().toString()
        val files = getJson(client, token, url).optJSONArray("files") ?: return null
        if (files.length() == 0) return null
        return files.getJSONObject(0).optJSONObject("appProperties")?.optString("reset_at")?.ifEmpty { null }
    }

    fun downloadJson(client: OkHttpClient, token: String, fileId: String): JSONObject =
        execute(client, authed(token).url("$DRIVE/files/$fileId?alt=media").get().build())
            .use { JSONObject(it.body!!.string()) }

    fun downloadBytes(client: OkHttpClient, token: String, fileId: String): ByteArray =
        execute(client, authed(token).url("$DRIVE/files/$fileId?alt=media").get().build())
            .use { it.body!!.bytes() }

    /** Google's own small preview of a file at about `sizePx` (a few KB
     * instead of the whole photo), or null if Drive hasn't made one yet. */
    fun downloadThumbnail(client: OkHttpClient, token: String, fileId: String, sizePx: Int): ByteArray? {
        val link = getJson(client, token, "$DRIVE/files/$fileId?fields=thumbnailLink")
            .optString("thumbnailLink").ifEmpty { return null }
        // thumbnailLink ends in "=s220"; ask for the size the screen needs
        val sized = link.replace(Regex("=s\\d+$"), "=s$sizePx")
        return execute(client, authed(token).url(sized).get().build()).use { it.body?.bytes() }
    }

    private fun listFiles(client: OkHttpClient, token: String, query: String, max: Int): List<JSONObject> {
        val out = mutableListOf<JSONObject>()
        var pageToken: String? = null
        do {
            val builder = "$DRIVE/files".toHttpUrl().newBuilder()
                .addQueryParameter("q", query)
                .addQueryParameter("orderBy", "createdTime desc")
                .addQueryParameter("pageSize", "100")
                .addQueryParameter("fields", "nextPageToken,files(id,name,createdTime,appProperties)")
            pageToken?.let { builder.addQueryParameter("pageToken", it) }
            val body = getJson(client, token, builder.build().toString())
            val files = body.optJSONArray("files") ?: JSONArray()
            for (i in 0 until files.length()) out.add(files.getJSONObject(i))
            pageToken = body.optString("nextPageToken").ifEmpty { null }
        } while (pageToken != null && out.size < max)
        return out.take(max)
    }

    /**
     * The user's own uploads (their /images/<email>/ folder), newest first,
     * each merged with its result from /results/<email>/ (if Spark has
     * written one), as items shaped like the old GET /my/history:
     *   {image_id, drive_file_id, processed_file_id, location, uploaded_at,
     *    outcome, prediction: {predicted_class, prediction_confidence,
     *    risk_level} | null, error_message}
     * outcome follows the same rule the API used: failed -> pending (no
     * result yet) -> healthy -> diseased.
     */
    fun myHistory(client: OkHttpClient, token: String, folders: Folders, max: Int = 200): List<JSONObject> {
        val images = listFiles(
            client, token,
            "'${folders.imagesId}' in parents and trashed = false and mimeType != '$FOLDER_MIME'", max
        )
        val results = listFiles(
            client, token,
            "'${folders.resultsId}' in parents and trashed = false and mimeType = 'application/json'", max
        )
        val resultByImage = results.associateBy {
            it.optJSONObject("appProperties")?.optString("image_id").orEmpty()
        }

        val items = mutableListOf<JSONObject>()
        val seen = mutableSetOf<String>()
        for (image in images) {
            val props = image.optJSONObject("appProperties") ?: JSONObject()
            val imageId = props.optString("image_id").ifEmpty { image.optString("name").substringBeforeLast('.') }
            seen.add(imageId)
            items.add(historyItem(imageId, image.getString("id"), props.optString("location"),
                image.optString("createdTime"), resultByImage[imageId]?.optJSONObject("appProperties")))
        }
        // results whose photo is no longer listed (e.g. deleted from Drive) still show up
        for ((imageId, result) in resultByImage) {
            if (imageId.isEmpty() || imageId in seen) continue
            val props = result.optJSONObject("appProperties") ?: JSONObject()
            items.add(historyItem(imageId, props.optString("drive_image_file_id"), props.optString("location"),
                props.optString("uploaded_at"), props))
        }
        return items.sortedByDescending { it.optString("uploaded_at") }
    }

    /**
     * Every result Spark wrote or updated in /results/<email>/ after
     * `sinceIso` (null = all of them), as history items, plus the newest
     * modifiedTime seen (pass it back next time). Lets the phone keep the
     * results of thousands of photos up to date with a handful of requests
     * (1000 per page), instead of the newest 200 only.
     */
    fun resultsSince(
        client: OkHttpClient, token: String, folders: Folders, sinceIso: String?,
    ): Pair<List<JSONObject>, String?> {
        var query = "'${folders.resultsId}' in parents and trashed = false and mimeType = 'application/json'"
        if (sinceIso != null) query += " and modifiedTime > '${q(sinceIso)}'"
        val items = mutableListOf<JSONObject>()
        var newest = sinceIso
        var pageToken: String? = null
        do {
            val builder = "$DRIVE/files".toHttpUrl().newBuilder()
                .addQueryParameter("q", query)
                .addQueryParameter("pageSize", "1000")
                .addQueryParameter("fields", "nextPageToken,files(id,modifiedTime,appProperties)")
            pageToken?.let { builder.addQueryParameter("pageToken", it) }
            val body = getJson(client, token, builder.build().toString())
            val files = body.optJSONArray("files") ?: JSONArray()
            for (i in 0 until files.length()) {
                val f = files.getJSONObject(i)
                val props = f.optJSONObject("appProperties") ?: continue
                val imageId = props.optString("image_id")
                if (imageId.isEmpty()) continue
                items.add(historyItem(imageId, props.optString("drive_image_file_id"), props.optString("location"),
                    props.optString("uploaded_at"), props))
                val modified = f.optString("modifiedTime")
                if (newest == null || modified > newest) newest = modified
            }
            pageToken = body.optString("nextPageToken").ifEmpty { null }
        } while (pageToken != null)
        return items to newest
    }

    private fun historyItem(
        imageId: String, driveFileId: String, imageLocation: String, createdTime: String, result: JSONObject?,
    ): JSONObject {
        val status = result?.optString("status").orEmpty()
        val predictedClass = result?.optString("predicted_class").orEmpty()
        val outcome = when {
            status == "FAILED" -> "failed"
            status != "COMPLETED" -> "pending"
            predictedClass.equals("healthy", ignoreCase = true) -> "healthy"
            else -> "diseased"
        }
        val location = result?.optString("location")?.ifEmpty { null } ?: imageLocation.ifEmpty { null }
        return JSONObject()
            .put("image_id", imageId)
            .put("drive_file_id", driveFileId)
            .put("processed_file_id", result?.optString("processed_file_id").orEmpty())
            .put("location", location ?: JSONObject.NULL)
            .put("uploaded_at", result?.optString("uploaded_at")?.ifEmpty { null } ?: createdTime)
            .put("outcome", outcome)
            .put("prediction", if (status == "COMPLETED") JSONObject()
                .put("predicted_class", predictedClass.ifEmpty { "unknown" })
                .put("prediction_confidence", result?.optString("prediction_confidence")?.toDoubleOrNull() ?: 0.0)
                .put("risk_level", result?.optString("risk_level").orEmpty().ifEmpty { "UNKNOWN" })
                else JSONObject.NULL)
            .put("error_message", result?.optString("error_message")?.ifEmpty { null } ?: JSONObject.NULL)
    }
}

package com.cropdisease.app

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.location.Location
import android.location.LocationManager
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.CancellationSignal
import android.os.Handler
import android.os.Looper
import android.provider.DocumentsContract
import android.provider.OpenableColumns
import android.view.Gravity
import android.view.View
import android.widget.AdapterView
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.TextView
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import androidx.core.location.LocationManagerCompat
import com.cropdisease.app.databinding.ActivityMainBinding
import okhttp3.OkHttpClient
import org.json.JSONObject
import java.io.File
import java.util.UUID
import java.util.concurrent.Executors

/**
 * USER screen: pick a location from a fixed dropdown (defined in
 * res/values/strings.xml — "Other" reveals a free-text field for anywhere
 * not listed), take photos, choose photos, or choose a whole folder of
 * photos — any number — then "Upload": the photos go into the upload queue
 * saved on the phone (UploadQueue) and UploadWorker sends them to the
 * farmer's own Google Drive /images folder in the background, with the
 * user's own Google sign-in. It keeps going if this screen or the app is
 * closed. Processing happens asynchronously on the laptop: Drive watcher
 * -> Kafka -> Spark -> Drive /results.
 *
 * Up to INLINE_RESULTS_MAX photos, each photo's result is also shown right
 * here (original and processed photo side by side), polled from Drive
 * /results. Bigger uploads show progress here and their results in History.
 *
 * Location is a free-text display/filter label carried on every image in
 * the upload, not an access-control boundary. It is required.
 *
 * GPS: gallery photos are read unredacted (ACCESS_MEDIA_LOCATION +
 * MediaStore.setRequireOriginal) so their own EXIF GPS tags survive. Photos
 * taken with the in-app camera are also tagged with the phone's position at
 * capture time (appProperties gps_*), which the laptop uses when the photo
 * itself has no GPS tags. Gallery photos never get the phone's CURRENT
 * position — they may have been taken somewhere else.
 */
class MainActivity : AppCompatActivity() {

    companion object {
        // Uploads this small also show each photo's result on this screen
        // (original + processed side by side); bigger ones are followed in History.
        private const val INLINE_RESULTS_MAX = 20
        // Thumbnails drawn in the selection strip; the rest is one "+N" tile,
        // so selecting thousands of photos doesn't decode thousands of images.
        private const val STRIP_PREVIEW_COUNT = 12
        // Size of the original / processed images in the side-by-side result.
        private const val ORIGINAL_PREVIEW_PX = 500
        // A cached phone position this recent is good enough for a farm photo.
        private const val MAX_LAST_FIX_AGE_MS = 2 * 60 * 1000L
        private const val LOCATION_TIMEOUT_MS = 20_000L
        private const val POLL_INTERVAL_MS = 2000L
        private const val PROGRESS_REFRESH_MS = 1000L
    }

    // Not a data class: photos are removed from the strip by identity, and
    // deviceLocation arrives a few seconds after a camera photo is added.
    private class PendingPhoto(
        val uri: Uri,
        val displayName: String,
        val mimeType: String,
        /** In-app camera photo: our own file in UploadQueue.ownedDir. */
        val ownedFile: File? = null,
        /** Found through "Choose folder": read through that folder's permission. */
        val treeUri: Uri? = null,
    ) {
        var deviceLocation: Location? = null
        /** Gallery photo whose read permission was kept (persisted). */
        var accessPersisted = false
    }

    private lateinit var binding: ActivityMainBinding
    private lateinit var session: SessionStore
    private val queue by lazy { UploadQueue.get(this) }
    private val selectedPhotos = mutableListOf<PendingPhoto>()
    // Upload history saved on the phone (see HistoryStore) — Home/History read it.
    private val history by lazy { HistoryStore(this, session.email.orEmpty()) }
    private var cameraPhotoFile: File? = null

    // image_id -> the row showing that photo's live status (small uploads only).
    private val resultViews = mutableMapOf<String, OutcomeUi.Row>()
    private val resultNames = mutableMapOf<String, String>()

    private val httpClient = OkHttpClient()
    private val pollHandler = Handler(Looper.getMainLooper())
    private var pollRunnable: Runnable? = null
    private var progressRunnable: Runnable? = null

    // Phone-side thumbnails (strip + "original" in each result row), decoded
    // in the background and kept per uri+size; touched on the main thread only.
    private val decodeExecutor = Executors.newFixedThreadPool(2)
    private val localBitmaps = mutableMapOf<String, Bitmap>()

    // ---- Activity result launchers ----

    private val cameraLauncher = registerForActivityResult(
        ActivityResultContracts.TakePicture()
    ) { success ->
        val file = cameraPhotoFile ?: return@registerForActivityResult
        if (success && file.length() > 0) {
            val uri = FileProvider.getUriForFile(this, "$packageName.fileprovider", file)
            addPhotos(listOf(PendingPhoto(uri, file.name, "image/jpeg", ownedFile = file)))
                .firstOrNull()?.let { captureDeviceLocation(it) }
        } else {
            file.delete()
        }
    }

    // OpenDocument rather than GetContent: GetContent can route through the
    // system photo picker, which always strips GPS tags from the photo.
    private val galleryLauncher = registerForActivityResult(
        ActivityResultContracts.OpenMultipleDocuments()
    ) { uris: List<Uri> ->
        addPhotos(uris.map { uri ->
            val (name, mime) = uriMeta(uri)
            PendingPhoto(uri, name, mime).apply {
                // Keep read access after this screen closes, so the
                // background upload can still read it. Android limits how
                // many an app may keep; past that the photo is copied at
                // "Upload" time instead (see toQueueItem).
                accessPersisted = try {
                    contentResolver.takePersistableUriPermission(uri, Intent.FLAG_GRANT_READ_URI_PERMISSION)
                    true
                } catch (e: Exception) {
                    false
                }
            }
        })
    }

    private val folderLauncher = registerForActivityResult(
        ActivityResultContracts.OpenDocumentTree()
    ) { tree: Uri? ->
        if (tree == null) return@registerForActivityResult
        try {
            contentResolver.takePersistableUriPermission(tree, Intent.FLAG_GRANT_READ_URI_PERMISSION)
        } catch (e: Exception) {
            toast(getString(R.string.error_folder_access)); return@registerForActivityResult
        }
        setStatus(getString(R.string.status_reading_folder))
        Thread {
            val found = try { listFolderImages(tree) } catch (e: Exception) { emptyList() }
            runOnUiThread {
                if (isDestroyed) return@runOnUiThread
                setStatus("")
                if (found.isEmpty()) toast(getString(R.string.toast_folder_no_photos))
                addPhotos(found)
            }
        }.start()
    }

    private val cameraPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { _ ->
        // Location is optional: without it the photo is still uploaded, just untagged.
        if (isGranted(Manifest.permission.CAMERA)) launchCamera()
        else toast(getString(R.string.toast_camera_permission_required))
    }

    private val mediaLocationPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { _ -> pendingPickAction?.invoke(); pendingPickAction = null }  // opens either way; denied just means no GPS tags
    private var pendingPickAction: (() -> Unit)? = null

    // Only for the upload notification; the upload runs whether or not it's granted.
    private val notificationPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { _ -> }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        session = SessionStore(this)

        if (!session.isLoggedIn) {
            startActivity(Intent(this, LoginActivity::class.java))
            finish()
            return
        }

        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.loggedInAsText.text = getString(R.string.logged_in_as, session.username, session.role)
        binding.takePhotoButton.setOnClickListener { onTakePhotoClicked() }
        binding.pickPhotoButton.setOnClickListener { withMediaLocation { launchGallery() } }
        binding.pickFolderButton.setOnClickListener { withMediaLocation { folderLauncher.launch(null) } }
        binding.clearPhotosButton.setOnClickListener {
            val removed = selectedPhotos.toList()
            selectedPhotos.clear()
            localBitmaps.clear()
            releasePhotos(removed)
            refreshPhotoStrip()
        }
        binding.uploadButton.setOnClickListener { onUploadClicked() }
        binding.retryFailedButton.setOnClickListener { onRetryFailedClicked() }
        binding.openHistoryButton.setOnClickListener {
            startActivity(Intent(this, HistoryActivity::class.java))
        }

        binding.locationSpinner.onItemSelectedListener = object : AdapterView.OnItemSelectedListener {
            override fun onItemSelected(parent: AdapterView<*>?, view: View?, position: Int, id: Long) {
                val isOther = binding.locationSpinner.selectedItem?.toString() == "Other"
                binding.otherLocationLayout.visibility = if (isOther) View.VISIBLE else View.GONE
            }

            override fun onNothingSelected(parent: AdapterView<*>?) {}
        }

        refreshPhotoStrip()
    }

    override fun onResume() {
        super.onResume()
        if (!::binding.isInitialized) return
        UploadWorker.resumeIfPending(this)
        refreshUploadProgress()
    }

    override fun onPause() {
        super.onPause()
        progressRunnable?.let { pollHandler.removeCallbacks(it) }
        progressRunnable = null
    }

    override fun onDestroy() {
        super.onDestroy()
        pollRunnable?.let { pollHandler.removeCallbacks(it) }
        decodeExecutor.shutdownNow()
    }

    // ---- Location ----

    /** Resolves the current dropdown/free-text selection into the string
     * to upload — null for the placeholder (no location chosen), the
     * dropdown's own text for a listed municipality, or the free-text
     * field's (trimmed) content when "Other" is selected. */
    private fun selectedLocation(): String? {
        val selected = binding.locationSpinner.selectedItem?.toString() ?: return null
        return when (selected) {
            "Select Location" -> null
            "Other" -> binding.otherLocationInput.text?.toString()?.trim()?.takeIf { it.isNotEmpty() }
            else -> selected
        }
    }

    // ---- Photo selection (camera adds one at a time, gallery multi-selects, folder adds all) ----

    private fun isGranted(permission: String) =
        ContextCompat.checkSelfPermission(this, permission) == PackageManager.PERMISSION_GRANTED

    private fun onTakePhotoClicked() {
        val missing = listOf(
            Manifest.permission.CAMERA,
            Manifest.permission.ACCESS_FINE_LOCATION,
            Manifest.permission.ACCESS_COARSE_LOCATION,
        ).filterNot { isGranted(it) }
        if (missing.isEmpty()) launchCamera() else cameraPermissionLauncher.launch(missing.toTypedArray())
    }

    /** Asks for ACCESS_MEDIA_LOCATION first (so the photos' own GPS tags
     * survive), then opens the picker either way. */
    private fun withMediaLocation(open: () -> Unit) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && !isGranted(Manifest.permission.ACCESS_MEDIA_LOCATION)) {
            pendingPickAction = open
            mediaLocationPermissionLauncher.launch(Manifest.permission.ACCESS_MEDIA_LOCATION)
        } else {
            open()
        }
    }

    private fun launchGallery() = galleryLauncher.launch(arrayOf("image/jpeg", "image/png"))

    /** Every JPEG/PNG in the chosen folder and its sub-folders, by name. */
    private fun listFolderImages(tree: Uri): List<PendingPhoto> {
        val out = mutableListOf<PendingPhoto>()
        val folders = ArrayDeque<String>().apply { add(DocumentsContract.getTreeDocumentId(tree)) }
        val columns = arrayOf(
            DocumentsContract.Document.COLUMN_DOCUMENT_ID,
            DocumentsContract.Document.COLUMN_DISPLAY_NAME,
            DocumentsContract.Document.COLUMN_MIME_TYPE,
        )
        while (folders.isNotEmpty()) {
            val children = DocumentsContract.buildChildDocumentsUriUsingTree(tree, folders.removeLast())
            contentResolver.query(children, columns, null, null, null)?.use { c ->
                while (c.moveToNext()) {
                    val docId = c.getString(0) ?: continue
                    val name = c.getString(1) ?: "photo.jpg"
                    when (val mime = c.getString(2).orEmpty()) {
                        DocumentsContract.Document.MIME_TYPE_DIR -> folders.add(docId)
                        "image/jpeg", "image/png" -> out.add(PendingPhoto(
                            DocumentsContract.buildDocumentUriUsingTree(tree, docId), name, mime, treeUri = tree
                        ))
                    }
                }
            }
        }
        return out.sortedBy { it.displayName.lowercase() }
    }

    /** Records where the phone is right now on a photo just taken with the
     * in-app camera: a recent cached fix immediately, then a fresh one when
     * it arrives (within LOCATION_TIMEOUT_MS). No permission, location
     * switched off, or no fix in time -> the photo simply has no device GPS. */
    private fun captureDeviceLocation(photo: PendingPhoto) {
        val fine = isGranted(Manifest.permission.ACCESS_FINE_LOCATION)
        if (!fine && !isGranted(Manifest.permission.ACCESS_COARSE_LOCATION)) return
        val manager = getSystemService(LOCATION_SERVICE) as LocationManager
        val provider = when {
            fine && manager.isProviderEnabled(LocationManager.GPS_PROVIDER) -> LocationManager.GPS_PROVIDER
            manager.isProviderEnabled(LocationManager.NETWORK_PROVIDER) -> LocationManager.NETWORK_PROVIDER
            else -> return
        }
        try {
            manager.getLastKnownLocation(provider)
                ?.takeIf { System.currentTimeMillis() - it.time < MAX_LAST_FIX_AGE_MS }
                ?.let { photo.deviceLocation = it }
            val cancel = CancellationSignal()
            pollHandler.postDelayed({ cancel.cancel() }, LOCATION_TIMEOUT_MS)
            LocationManagerCompat.getCurrentLocation(
                manager, provider, cancel, ContextCompat.getMainExecutor(this)
            ) { fix -> if (fix != null) photo.deviceLocation = fix }
        } catch (e: SecurityException) {
            // permission revoked between the check and the call
        }
    }

    private fun launchCamera() {
        // filesDir (not cacheDir): the photo may wait in the upload queue
        // for a while, and Android can clear cacheDir at any time.
        val photoFile = File(UploadQueue.ownedDir(this), "leaf_${System.currentTimeMillis()}.jpg")
        photoFile.parentFile?.mkdirs()
        cameraPhotoFile = photoFile
        cameraLauncher.launch(FileProvider.getUriForFile(this, "$packageName.fileprovider", photoFile))
    }

    /** Adds photos not already selected; returns the ones added. */
    private fun addPhotos(photos: List<PendingPhoto>): List<PendingPhoto> {
        val existing = selectedPhotos.mapTo(HashSet()) { it.uri.toString() }
        val added = photos.filter { existing.add(it.uri.toString()) }
        if (added.isEmpty()) return added
        selectedPhotos.addAll(added)
        binding.batchResultsContainer.removeAllViews()
        resultViews.clear()
        refreshPhotoStrip()
        return added
    }

    /** Lets go of photos removed from the selection without being uploaded. */
    private fun releasePhotos(photos: List<PendingPhoto>) {
        val keptTrees = selectedPhotos.mapNotNullTo(HashSet()) { it.treeUri?.toString() }
        val trees = mutableSetOf<Uri>()
        for (p in photos) {
            p.ownedFile?.delete()
            if (p.accessPersisted) try {
                contentResolver.releasePersistableUriPermission(p.uri, Intent.FLAG_GRANT_READ_URI_PERMISSION)
            } catch (e: Exception) { }
            p.treeUri?.takeIf { it.toString() !in keptTrees }?.let { trees.add(it) }
        }
        if (trees.isEmpty()) return
        Thread {
            for (tree in trees) {
                if (queue.usesTree(tree.toString())) continue  // the background upload still reads it
                try {
                    contentResolver.releasePersistableUriPermission(tree, Intent.FLAG_GRANT_READ_URI_PERMISSION)
                } catch (e: Exception) { }
            }
        }.start()
    }

    /** Resolves a content:// (or FileProvider) uri's display name and MIME
     * type, falling back to a generated name / image/jpeg when the
     * provider doesn't report one. */
    private fun uriMeta(uri: Uri): Pair<String, String> {
        val mime = contentResolver.getType(uri) ?: "image/jpeg"
        var name = "photo_${System.currentTimeMillis()}.jpg"
        contentResolver.query(uri, arrayOf(OpenableColumns.DISPLAY_NAME), null, null, null)?.use { c ->
            if (c.moveToFirst()) {
                val idx = c.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                if (idx >= 0) c.getString(idx)?.let { name = it }
            }
        }
        return name to mime
    }

    /** Decodes a uri into a bitmap downsampled to roughly `reqSizePx`, so
     * thumbnails don't hold full-resolution bitmaps in memory. */
    private fun decodeSampledBitmap(uri: Uri, reqSizePx: Int): Bitmap? = try {
        val bounds = BitmapFactory.Options().apply { inJustDecodeBounds = true }
        contentResolver.openInputStream(uri)?.use { BitmapFactory.decodeStream(it, null, bounds) }
        var sample = 1
        while (bounds.outWidth / sample > reqSizePx * 2 || bounds.outHeight / sample > reqSizePx * 2) {
            sample *= 2
        }
        val opts = BitmapFactory.Options().apply { inSampleSize = sample }
        contentResolver.openInputStream(uri)?.use { BitmapFactory.decodeStream(it, null, opts) }
    } catch (e: Exception) {
        null
    }

    /** Decodes a phone photo off the main thread and shows it in `target`.
     * Each photo is decoded once per size. */
    private fun setLocalBitmap(target: ImageView, uri: Uri, sizePx: Int) {
        val key = "$sizePx|$uri"
        localBitmaps[key]?.let { target.setImageBitmap(it); return }
        decodeExecutor.execute {
            val bitmap = decodeSampledBitmap(uri, sizePx) ?: return@execute
            runOnUiThread {
                if (isDestroyed) return@runOnUiThread
                localBitmaps[key] = bitmap
                target.setImageBitmap(bitmap)
            }
        }
    }

    private fun refreshPhotoStrip() {
        binding.photoStripContainer.removeAllViews()
        val thumbSizePx = (72 * resources.displayMetrics.density).toInt()
        val marginPx = (6 * resources.displayMetrics.density).toInt()

        for (photo in selectedPhotos.take(STRIP_PREVIEW_COUNT)) {
            val thumb = ImageView(this).apply {
                layoutParams = LinearLayout.LayoutParams(thumbSizePx, thumbSizePx).apply {
                    marginEnd = marginPx
                }
                scaleType = ImageView.ScaleType.CENTER_CROP
                clipToOutline = true
                background = ContextCompat.getDrawable(this@MainActivity, R.drawable.bg_thumb_placeholder)
                contentDescription = getString(R.string.content_description_remove_photo, photo.displayName)
                setLocalBitmap(this, photo.uri, thumbSizePx)
                setOnClickListener {
                    selectedPhotos.remove(photo)
                    releasePhotos(listOf(photo))
                    refreshPhotoStrip()
                }
            }
            binding.photoStripContainer.addView(thumb)
        }
        val more = selectedPhotos.size - STRIP_PREVIEW_COUNT
        if (more > 0) {
            binding.photoStripContainer.addView(TextView(this).apply {
                layoutParams = LinearLayout.LayoutParams(thumbSizePx, thumbSizePx)
                gravity = Gravity.CENTER
                background = ContextCompat.getDrawable(this@MainActivity, R.drawable.bg_thumb_placeholder)
                setTextColor(ContextCompat.getColor(this@MainActivity, R.color.text_primary))
                textSize = 16f
                text = getString(R.string.strip_more_photos, more)
            })
        }

        binding.selectedCountText.text = when (selectedPhotos.size) {
            0 -> getString(R.string.no_photos_selected)
            1 -> getString(R.string.photos_selected_one)
            else -> getString(R.string.photos_selected_other, selectedPhotos.size)
        }
        binding.uploadButton.isEnabled = selectedPhotos.isNotEmpty()
    }

    // ---- Upload: into the queue saved on the phone; UploadWorker sends it ----

    private fun onUploadClicked() {
        if (selectedPhotos.isEmpty()) { toast(getString(R.string.toast_pick_photo_first)); return }
        if (binding.locationSpinner.selectedItem?.toString() == "Other" &&
            binding.otherLocationInput.text?.toString()?.trim().isNullOrEmpty()) {
            toast(getString(R.string.toast_enter_location)); return
        }
        // Required: a photo without a municipality can't be placed on the map.
        val location = selectedLocation() ?: run { toast(getString(R.string.toast_select_location)); return }
        val email = session.email ?: return
        val photos = selectedPhotos.toList()

        if (Build.VERSION.SDK_INT >= 33 && !isGranted(Manifest.permission.POST_NOTIFICATIONS)) {
            notificationPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
        }

        binding.uploadButton.isEnabled = false
        binding.batchResultsContainer.removeAllViews()
        resultViews.clear()
        resultNames.clear()
        setStatus(getString(R.string.status_adding_to_queue, photos.size))

        Thread {
            val items = photos.map { toQueueItem(it, email, location) }
            queue.enqueue(items)
            runOnUiThread {
                if (isDestroyed) return@runOnUiThread
                UploadWorker.start(this)
                selectedPhotos.clear()   // now owned by the queue — not released
                refreshPhotoStrip()
                setStatus("")
                if (items.size <= INLINE_RESULTS_MAX) showInlineRows(photos.zip(items))
                refreshUploadProgress()
            }
        }.start()
    }

    /** A queue row for one selected photo. Its image_id is fixed here, for
     * every retry. A gallery photo whose read access couldn't be kept is
     * copied now — byte for byte, while the picker's permission still holds. */
    private fun toQueueItem(photo: PendingPhoto, email: String, location: String): UploadQueue.Item {
        val imageId = UUID.randomUUID().toString()
        var uri = photo.uri.toString()
        var owned = false
        if (photo.ownedFile != null) {
            uri = Uri.fromFile(photo.ownedFile).toString()
            owned = true
        } else if (photo.treeUri == null && !photo.accessPersisted) {
            DriveUploader.readPhotoBytes(this, photo.uri)?.let { bytes ->
                val ext = if (photo.mimeType == "image/png") "png" else "jpg"
                val copy = File(UploadQueue.ownedDir(this), "$imageId.$ext")
                try {
                    copy.parentFile?.mkdirs()
                    copy.writeBytes(bytes)
                    uri = Uri.fromFile(copy).toString()
                    owned = true
                } catch (e: Exception) {
                    copy.delete()  // no space: try the original uri, rejected later if unreadable
                }
            }
        }
        val fix = photo.deviceLocation
        return UploadQueue.Item(
            imageId = imageId,
            userEmail = email,
            uri = uri,
            displayName = photo.displayName,
            mimeType = photo.mimeType,
            location = location,
            gpsLat = fix?.latitude,
            gpsLon = fix?.longitude,
            gpsAccM = fix?.takeIf { it.hasAccuracy() }?.accuracy,
            ownedFile = owned,
            treeUri = photo.treeUri?.toString(),
        )
    }

    private fun onRetryFailedClicked() {
        val email = session.email ?: return
        Thread {
            val n = queue.retryFailed(email)
            runOnUiThread {
                if (n > 0) UploadWorker.start(this)
                refreshUploadProgress()
            }
        }.start()
    }

    /** Re-reads the queue's totals every PROGRESS_REFRESH_MS while this
     * screen is visible. */
    private fun refreshUploadProgress() {
        progressRunnable?.let { pollHandler.removeCallbacks(it) }
        val email = session.email ?: return
        Thread {
            val counts = queue.counts(email)
            runOnUiThread {
                if (isDestroyed) return@runOnUiThread
                showUploadProgress(counts)
                val next = Runnable { refreshUploadProgress() }
                progressRunnable = next
                pollHandler.postDelayed(next, PROGRESS_REFRESH_MS)
            }
        }.start()
    }

    private fun showUploadProgress(c: UploadQueue.Counts) {
        if (c.total == 0) {
            binding.uploadProgressCard.visibility = View.GONE
            return
        }
        binding.uploadProgressCard.visibility = View.VISIBLE
        binding.uploadProgressIndicator.max = c.total
        binding.uploadProgressIndicator.progress = c.done
        val notSent = c.failed + c.rejected
        binding.uploadProgressText.text = if (notSent == 0)
            getString(R.string.upload_progress, c.done, c.total)
        else
            getString(R.string.upload_progress_with_failed, c.done, c.total, notSent)
        binding.uploadProgressHint.text = getString(
            if (c.queued > 0) R.string.upload_hint_running else R.string.upload_hint_done
        )
        binding.retryFailedButton.visibility = if (c.failed > 0 && c.queued == 0) View.VISIBLE else View.GONE
        binding.retryFailedButton.text = getString(R.string.btn_retry_failed, c.failed)
    }

    // ---- Small uploads: each photo's result right here ----

    private fun showInlineRows(rows: List<Pair<PendingPhoto, UploadQueue.Item>>) {
        for ((photo, item) in rows) {
            val row = OutcomeUi.addComparisonRow(
                this, binding.batchResultsContainer,
                getString(R.string.row_waiting_to_upload, item.displayName),
                OutcomeUi.colorFor(this, "pending")
            )
            // Original: straight from the phone (no need to download it back).
            row.thumbnail?.let { setLocalBitmap(it, photo.uri, ORIGINAL_PREVIEW_PX) }
            resultViews[item.imageId] = row
            resultNames[item.imageId] = item.displayName
        }
        binding.progressBar.visibility = View.VISIBLE
        // ~1 minute after the last photo is on Drive, plus ~2s per photo:
        // a bigger upload waits its turn in Spark.
        pollInline(attemptsLeft = 30 + rows.size)
    }

    private fun pollInline(attemptsLeft: Int) {
        if (resultViews.isEmpty()) {
            binding.progressBar.visibility = View.GONE
            return
        }
        if (attemptsLeft <= 0) {
            binding.progressBar.visibility = View.GONE
            setStatus(getString(R.string.status_still_processing))
            for (row in resultViews.values) {
                row.label.text = getString(R.string.row_suffix_still_processing, row.label.text)
            }
            resultViews.clear()
            return
        }

        // Queue state (still uploading? failed?) from the phone, results
        // from Google Drive /results/<image_id>.json — one query per 20 photos.
        // The next round is scheduled only when this one has finished.
        val waiting = resultViews.keys.toList()
        Thread {
            val states = queue.items(waiting)
            val uploaded = waiting.filter { states[it]?.state == UploadQueue.UPLOADED }
            val token = if (uploaded.isEmpty()) null else DriveAuth.accessTokenBlocking(this)
            val finished = mutableMapOf<String, JSONObject>()
            if (token != null) try {
                val folders = session.folders(httpClient, token)
                if (folders != null) {
                    for ((imageId, fileId) in DriveRest.findResults(httpClient, token, folders.resultsId, uploaded)) {
                        finished[imageId] = DriveRest.downloadJson(httpClient, token, fileId)
                    }
                    for ((imageId, json) in finished) history.applyResult(imageId, json)
                }
            } catch (e: Exception) { /* retried next round */ }

            runOnUiThread {
                if (isDestroyed) return@runOnUiThread
                val stillUploading = waiting.any { states[it]?.state == UploadQueue.QUEUED }
                // The timeout only counts once every photo is on Drive.
                val next = if (stillUploading) attemptsLeft else attemptsLeft - 1
                pollRunnable = Runnable { pollInline(next) }.also { pollHandler.postDelayed(it, POLL_INTERVAL_MS) }

                for (imageId in waiting) {
                    val row = resultViews[imageId] ?: continue
                    val name = resultNames[imageId].orEmpty()
                    val item = states[imageId]
                    val result = finished[imageId]
                    when {
                        item == null -> Unit
                        item.state == UploadQueue.FAILED || item.state == UploadQueue.REJECTED -> {
                            row.label.text = getString(
                                R.string.row_failed, name, item.error ?: getString(R.string.error_unknown_error)
                            )
                            OutcomeUi.setAccent(row, OutcomeUi.colorFor(this, "failed"))
                            resultViews.remove(imageId)
                        }
                        result?.optString("status") == "COMPLETED" -> {
                            showResultRow(row, result.getJSONObject("prediction"))
                            // Processed (marked-up) image, written by Spark to /results/<email>/
                            if (!result.isNull("processed_file_id")) {
                                row.processed?.let { target ->
                                    ThumbnailLoader.load(
                                        httpClient, token, result.optString("processed_file_id"), target,
                                        maxPx = ORIGINAL_PREVIEW_PX,
                                    )
                                }
                            }
                            resultViews.remove(imageId)
                        }
                        result?.optString("status") == "FAILED" -> {
                            row.label.text = getString(
                                R.string.row_processing_failed,
                                result.optString("error_message", getString(R.string.error_unknown_error))
                            )
                            OutcomeUi.setAccent(row, OutcomeUi.colorFor(this, "failed"))
                            resultViews.remove(imageId)
                        }
                        item.state == UploadQueue.UPLOADED ->
                            row.label.text = getString(R.string.row_waiting_to_process, name)
                    }
                }
            }
        }.start()
    }

    private fun showResultRow(row: OutcomeUi.Row, prediction: JSONObject) {
        val diseaseClass = prediction.optString("predicted_class", "unknown")
        val confidence = prediction.optDouble("prediction_confidence", 0.0)
        val riskLevel = prediction.optString("risk_level", "UNKNOWN")
        val score = prediction.optDouble("risk_score", 0.0)
        val outcome = if (diseaseClass.lowercase() == "healthy") "healthy" else "diseased"

        row.label.text = if (outcome == "healthy")
            getString(R.string.result_healthy, confidence * 100)
        else
            getString(R.string.result_diseased, diseaseClass, confidence * 100, riskLevel, score)
        OutcomeUi.setAccent(row, OutcomeUi.colorFor(this, outcome))
    }

    // ---- Helpers ----

    private fun setStatus(message: String, isError: Boolean = false) {
        binding.statusText.text = message
        binding.statusText.setTextColor(
            ContextCompat.getColor(this, if (isError) R.color.error_text else R.color.text_secondary)
        )
    }

    private fun toast(message: String) {
        Toast.makeText(this, message, Toast.LENGTH_SHORT).show()
    }

}

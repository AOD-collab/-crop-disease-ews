package com.cropdisease.app

import android.content.Intent
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.View
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import com.cropdisease.app.databinding.ActivityHistoryBinding
import com.cropdisease.app.databinding.ItemStatTileBinding
import com.google.android.material.chip.Chip
import okhttp3.OkHttpClient
import org.json.JSONObject

/**
 * The signed-in user's own upload history, read from Google Drive
 * (DriveRest.myHistory: their photos in /images + Spark's results in
 * /results) — newest first, each tagged healthy/diseased/pending/failed,
 * with a KPI row of running totals and a filter chip row. Only the user's
 * own photos ('me' in owners) and results (user_email) are listed.
 *
 * The history is also saved on the phone (HistoryStore): the saved copy
 * shows instantly and offline, then Drive's list is merged in. The saved
 * copy keeps photos older than the newest 200 that one Drive read returns.
 */
class HistoryActivity : AppCompatActivity() {

    companion object {
        private const val PAGE_SIZE = 50
        private const val AUTO_REFRESH_MS = 30_000L
    }

    // label shown on the chip -> the item "outcome" it keeps (null = no filter, "All")
    private val filters by lazy {
        listOf(
            getString(R.string.filter_all) to null,
            getString(R.string.outcome_healthy_label) to "healthy",
            getString(R.string.outcome_diseased_label) to "diseased",
            getString(R.string.outcome_pending_label) to "pending",
            getString(R.string.outcome_failed_label) to "failed",
        )
    }

    private lateinit var binding: ActivityHistoryBinding
    private lateinit var session: SessionStore
    private val httpClient = OkHttpClient()
    private val history by lazy { HistoryStore(this, session.email.orEmpty()) }

    private var currentFilter: String? = null
    private var offset = 0
    private var total = 0

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        session = SessionStore(this)
        binding = ActivityHistoryBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.loadMoreButton.setOnClickListener { loadPage(append = true) }
        BottomNav.setup(this, binding.bottomNav, BottomNav.Tab.HISTORY)

        setUpStatTile(binding.tileHealthy, "●", getString(R.string.outcome_healthy_label), OutcomeUi.colorFor(this, "healthy"))
        setUpStatTile(binding.tileDiseased, "▲", getString(R.string.outcome_diseased_label), OutcomeUi.colorFor(this, "diseased"))
        setUpStatTile(binding.tilePending, "◐", getString(R.string.outcome_pending_label), OutcomeUi.colorFor(this, "pending"))
        setUpStatTile(binding.tileFailed, "✕", getString(R.string.outcome_failed_label), OutcomeUi.colorFor(this, "failed"))

        buildFilterChips()
        loadPage(append = false)
    }

    private fun setUpStatTile(tile: ItemStatTileBinding, icon: String, label: String, color: Int) {
        tile.tileIcon.text = icon
        tile.tileIcon.setTextColor(color)
        tile.tileValue.setTextColor(color)
        tile.tileLabel.text = label
    }

    private fun buildFilterChips() {
        binding.filterChipGroup.removeAllViews()
        for ((label, value) in filters) {
            val chip = Chip(this).apply {
                text = label
                isCheckable = true
                isChecked = value == currentFilter
                setOnCheckedChangeListener { _, checked ->
                    if (checked) {
                        currentFilter = value
                        loadPage(append = false)
                    }
                }
            }
            binding.filterChipGroup.addView(chip)
        }
    }

    // All of this user's uploads (+ results), read from Google Drive; the
    // filter chips and "Load more" page through this list on the phone.
    private var allItems: List<JSONObject> = emptyList()
    private var driveToken: String? = null

    private fun loadPage(append: Boolean) {
        if (append) {
            showNextPage()
            return
        }
        binding.historyProgressBar.visibility = View.VISIBLE
        binding.loadMoreButton.isEnabled = false

        // 1. What's saved on the phone, straight away (works offline; thumbnails
        //    come from the phone's saved copies too).
        val saved = history.load()
        showItems(saved)
        binding.historyStatusText.text =
            if (saved.isNotEmpty()) getString(R.string.history_refreshing) else ""  // not "no uploads" yet

        // 2. Then Google Drive, merged into the saved copy and shown again.
        Thread {
            val token = DriveAuth.accessTokenBlocking(this)
            if (token == null) {
                runOnUiThread {
                    binding.historyProgressBar.visibility = View.GONE
                    // Offline also gives no token: keep showing the saved history.
                    if (saved.isNotEmpty()) binding.historyStatusText.text = getString(R.string.history_saved_offline)
                    else sessionExpired()
                }
                return@Thread
            }
            val merged = try {
                readDrive(token)
            } catch (e: Exception) {
                runOnUiThread {
                    binding.historyProgressBar.visibility = View.GONE
                    when {
                        e is DriveRest.DriveHttpException && e.code == 401 -> sessionExpired()
                        saved.isNotEmpty() -> binding.historyStatusText.text = getString(R.string.history_saved_offline)
                        else -> binding.historyStatusText.text = getString(R.string.error_load_history, e.message)
                    }
                }
                return@Thread
            }
            runOnUiThread {
                binding.historyProgressBar.visibility = View.GONE
                driveToken = token
                showItems(merged)
            }
        }.start()
    }

    /** Merges Drive's current view into the saved copy and returns it. Blocking. */
    private fun readDrive(token: String): List<JSONObject> {
        val folders = session.folders(httpClient, token)
            ?: throw java.io.IOException(getString(R.string.error_drive_no_access))
        history.applyFactoryReset(httpClient, token, folders)
        history.merge(DriveRest.myHistory(httpClient, token, folders))
        // Results for photos older than the newest 200 (big uploads):
        // only what changed since the last time.
        history.syncResults(httpClient, token, folders)
        return history.load()
    }

    // ---- Auto refresh: re-read Drive every AUTO_REFRESH_MS while this
    // screen is on top, so new results (and a factory reset) show up
    // without leaving the screen. Stops in the background to save quota.

    private val refreshHandler = Handler(Looper.getMainLooper())
    private var autoRefreshing = false
    private val autoRefresh = object : Runnable {
        override fun run() {
            refreshQuietly()
            refreshHandler.postDelayed(this, AUTO_REFRESH_MS)
        }
    }

    override fun onResume() {
        super.onResume()
        refreshHandler.postDelayed(autoRefresh, AUTO_REFRESH_MS)  // onCreate already loaded once
    }

    override fun onPause() {
        super.onPause()
        refreshHandler.removeCallbacks(autoRefresh)
    }

    /** Like loadPage(), but silent: no spinner or error text (the next tick
     * simply tries again), and the list is only redrawn if something changed,
     * keeping the pages already opened with "Load more". */
    private fun refreshQuietly() {
        if (autoRefreshing) return
        autoRefreshing = true
        Thread {
            val merged = try {
                DriveAuth.accessTokenBlocking(this)?.let { token -> token to readDrive(token) }
            } catch (e: Exception) {
                null
            }
            runOnUiThread {
                autoRefreshing = false
                if (merged == null || isDestroyed) return@runOnUiThread
                val (token, items) = merged
                driveToken = token
                if (items.map { it.toString() } != allItems.map { it.toString() }) {
                    showItems(items, keepShown = offset)
                }
            }
        }.start()
    }

    /** Replaces the list (and KPI totals) with `items`, from the first page
     * (or, with keepShown, as many rows as were on screen before). */
    private fun showItems(items: List<JSONObject>, keepShown: Int = 0) {
        allItems = items
        offset = 0
        binding.historyListContainer.removeAllViews()
        renderSummary(JSONObject()
            .put("healthy", items.count { it.optString("outcome") == "healthy" })
            .put("diseased", items.count { it.optString("outcome") == "diseased" })
            .put("pending", items.count { it.optString("outcome") == "pending" })
            .put("failed", items.count { it.optString("outcome") == "failed" }))
        showNextPage(maxOf(PAGE_SIZE, keepShown))
    }

    private fun showNextPage(count: Int = PAGE_SIZE) {
        val filtered = allItems.filter { currentFilter == null || it.optString("outcome") == currentFilter }
        total = filtered.size
        val page = filtered.drop(offset).take(count)
        for (item in page) addHistoryRow(item)
        offset += page.size
        binding.loadMoreButton.isEnabled = true
        binding.loadMoreButton.visibility = if (offset < total) View.VISIBLE else View.GONE
        binding.historyStatusText.text = if (total == 0) getString(R.string.empty_no_uploads) else ""
    }

    private fun sessionExpired() {
        toast(getString(R.string.session_expired_toast))
        session.clearSession()
        startActivity(Intent(this@HistoryActivity, LoginActivity::class.java))
        finish()
    }

    private fun renderSummary(summary: JSONObject) {
        binding.tileHealthy.tileValue.text = summary.optInt("healthy").toString()
        binding.tileDiseased.tileValue.text = summary.optInt("diseased").toString()
        binding.tilePending.tileValue.text = summary.optInt("pending").toString()
        binding.tileFailed.tileValue.text = summary.optInt("failed").toString()
    }

    private fun addHistoryRow(item: JSONObject) {
        val outcome = item.optString("outcome")
        val row = OutcomeUi.addRow(
            this, binding.historyListContainer, rowText(item), OutcomeUi.colorFor(this, outcome),
            withThumbnail = true,
        )
        // The processed (marked-up) image once Spark has made one, else the original.
        val imageFileId = item.optString("processed_file_id").ifEmpty { item.optString("drive_file_id") }
        row.thumbnail?.let { ThumbnailLoader.load(httpClient, driveToken, imageFileId, it) }
    }

    private fun rowText(item: JSONObject): String {
        val unknownLocation = getString(R.string.unknown_location)
        val location = if (item.isNull("location")) unknownLocation else item.optString("location", unknownLocation)
        val uploadedAt = formatTimestamp(item.optString("uploaded_at", ""))
        val prediction = item.optJSONObject("prediction")

        val detail = when (item.optString("outcome")) {
            "healthy" -> getString(R.string.outcome_healthy_caps)
            "diseased" -> {
                val cls = prediction?.optString("predicted_class", "unknown") ?: "unknown"
                val confidence = prediction?.optDouble("prediction_confidence", 0.0) ?: 0.0
                val risk = prediction?.optString("risk_level", "UNKNOWN") ?: "UNKNOWN"
                getString(R.string.outcome_diseased_caps, cls, confidence * 100, risk)
            }
            "failed" -> {
                val error = if (item.isNull("error_message")) getString(R.string.error_unknown_error)
                    else item.optString("error_message", getString(R.string.error_unknown_error))
                getString(R.string.outcome_failed_caps, error)
            }
            else -> getString(R.string.outcome_pending_caps)
        }
        return "$location — $uploadedAt\n$detail"
    }

    /** Trims a server ISO timestamp ("2026-09-12T20:17:42.746397+00:00")
     * down to a readable "2026-09-12 20:17:42" without pulling in
     * java.time (this app's minSdk predates it without desugaring). */
    private fun formatTimestamp(iso: String): String = TimorTime.format(iso)

    private fun toast(message: String) {
        Toast.makeText(this, message, Toast.LENGTH_SHORT).show()
    }
}

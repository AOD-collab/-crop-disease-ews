package com.cropdisease.app

import android.content.Intent
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.View
import androidx.appcompat.app.AppCompatActivity
import com.cropdisease.app.databinding.ActivityHomeBinding
import okhttp3.OkHttpClient
import org.json.JSONObject

/**
 * Landing screen after login — a greeting, the big "Scan Maize for Disease"
 * CTA that launches the upload flow (MainActivity), and a short preview of
 * the user's most recent diagnoses (read from Google Drive, capped small), each
 * showing a real photo thumbnail. One of the three bottom-nav tabs
 * (Home / History / Settings) — see BottomNav.kt.
 */
class HomeActivity : AppCompatActivity() {

    companion object {
        private const val RECENT_COUNT = 5
        private const val AUTO_REFRESH_MS = 30_000L
    }

    private lateinit var binding: ActivityHomeBinding
    private lateinit var session: SessionStore
    private val httpClient = OkHttpClient()
    // Upload history saved on the phone (see HistoryStore).
    private val history by lazy { HistoryStore(this, session.email.orEmpty()) }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        session = SessionStore(this)

        if (!session.isLoggedIn) {
            startActivity(Intent(this, LoginActivity::class.java))
            finish()
            return
        }

        binding = ActivityHomeBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.greetingText.text = getString(
            R.string.home_greeting,
            session.username ?: getString(R.string.home_greeting_fallback_name)
        )
        binding.scanButton.setOnClickListener {
            startActivity(Intent(this, MainActivity::class.java))
        }

        BottomNav.setup(this, binding.bottomNav, BottomNav.Tab.HOME)
        // An upload left unfinished earlier (app killed, sign-in expired) carries on.
        UploadWorker.resumeIfPending(this)
    }

    override fun onResume() {
        super.onResume()
        // Picks up anything uploaded since this screen was last shown
        // (e.g. coming back from the upload flow).
        loadRecentDiagnoses()
        refreshHandler.removeCallbacks(autoRefresh)
        refreshHandler.postDelayed(autoRefresh, AUTO_REFRESH_MS)
    }

    // Google Drive access token for this screen's thumbnails.
    private var driveToken: String? = null

    private fun loadRecentDiagnoses() {
        // The copy saved on the phone first (instant, works offline) ...
        val saved = history.load().take(RECENT_COUNT)
        showRecent(saved)
        if (saved.isEmpty()) {
            binding.recentStatusText.visibility = View.VISIBLE
            binding.recentStatusText.text = getString(R.string.loading_ellipsis)
        }

        // ... then Google Drive (/images + /results), merged into it.
        Thread {
            val token = DriveAuth.accessTokenBlocking(this)
            if (token == null) {
                // Offline also gives no token: keep the saved list if there is one.
                runOnUiThread { if (saved.isEmpty()) goToLogin() }
                return@Thread
            }
            val items = try {
                val folders = session.folders(httpClient, token)
                if (folders == null) {
                    runOnUiThread { if (saved.isEmpty()) binding.recentStatusText.text = getString(R.string.error_drive_no_access) }
                    return@Thread
                }
                history.applyFactoryReset(httpClient, token, folders)
                history.merge(DriveRest.myHistory(httpClient, token, folders, max = RECENT_COUNT * 4))
                history.load().take(RECENT_COUNT)
            } catch (e: DriveRest.DriveHttpException) {
                runOnUiThread {
                    if (e.code == 401) goToLogin()
                    else if (saved.isEmpty()) binding.recentStatusText.text = getString(R.string.error_load_recent)
                }
                return@Thread
            } catch (e: Exception) {
                runOnUiThread { if (saved.isEmpty()) binding.recentStatusText.text = getString(R.string.error_load_recent) }
                return@Thread
            }
            runOnUiThread {
                driveToken = token
                showRecent(items)
                if (items.isEmpty()) {
                    binding.recentStatusText.visibility = View.VISIBLE
                    binding.recentStatusText.text = getString(R.string.empty_no_uploads_get_started)
                }
            }
        }.start()
    }

    // ---- Auto refresh: re-read Drive every AUTO_REFRESH_MS while Home is
    // on top, so results arriving (and a factory reset) show up without
    // leaving the screen. Stops in the background to save quota.

    private val refreshHandler = Handler(Looper.getMainLooper())
    private var autoRefreshing = false
    private var shownItems: List<String> = emptyList()
    private val autoRefresh = object : Runnable {
        override fun run() {
            refreshQuietly()
            refreshHandler.postDelayed(this, AUTO_REFRESH_MS)
        }
    }

    override fun onPause() {
        super.onPause()
        refreshHandler.removeCallbacks(autoRefresh)
    }

    /** Silent version of loadRecentDiagnoses(): no loading/error text (the
     * next tick tries again), and only redraws when something changed. */
    private fun refreshQuietly() {
        if (autoRefreshing) return
        autoRefreshing = true
        Thread {
            val fresh = try {
                DriveAuth.accessTokenBlocking(this)?.let { token ->
                    session.folders(httpClient, token)?.let { folders ->
                        history.applyFactoryReset(httpClient, token, folders)
                        history.merge(DriveRest.myHistory(httpClient, token, folders, max = RECENT_COUNT * 4))
                        token to history.load().take(RECENT_COUNT)
                    }
                }
            } catch (e: Exception) {
                null
            }
            runOnUiThread {
                autoRefreshing = false
                if (fresh == null || isDestroyed) return@runOnUiThread
                val (token, items) = fresh
                driveToken = token
                if (items.map { it.toString() } == shownItems) return@runOnUiThread
                showRecent(items)
                if (items.isEmpty()) {
                    binding.recentStatusText.visibility = View.VISIBLE
                    binding.recentStatusText.text = getString(R.string.empty_no_uploads_get_started)
                }
            }
        }.start()
    }

    private fun showRecent(items: List<JSONObject>) {
        shownItems = items.map { it.toString() }
        binding.recentContainer.removeAllViews()
        if (items.isNotEmpty()) binding.recentStatusText.visibility = View.GONE
        for (item in items) addRecentRow(item)
    }

    private fun goToLogin() {
        session.clearSession()
        startActivity(Intent(this@HomeActivity, LoginActivity::class.java))
        finish()
    }

    private fun addRecentRow(item: JSONObject) {
        val outcome = item.optString("outcome")
        val unknownLocation = getString(R.string.unknown_location)
        val location = if (item.isNull("location")) unknownLocation else item.optString("location", unknownLocation)
        val uploadedAt = TimorTime.format(item.optString("uploaded_at", ""), withTime = false)
        val prediction = item.optJSONObject("prediction")

        val diseasedFallback = getString(R.string.outcome_diseased_fallback)
        val detail = when (outcome) {
            "healthy" -> getString(R.string.outcome_healthy_label)
            "diseased" -> prediction?.optString("predicted_class", diseasedFallback) ?: diseasedFallback
            "failed" -> getString(R.string.outcome_processing_failed)
            else -> getString(R.string.outcome_still_processing)
        }

        val row = OutcomeUi.addRow(
            this, binding.recentContainer, "$location — $uploadedAt\n$detail",
            OutcomeUi.colorFor(this, outcome), withThumbnail = true,
        )
        // The processed (marked-up) image once Spark has made one, else the original.
        val imageFileId = item.optString("processed_file_id").ifEmpty { item.optString("drive_file_id") }
        row.thumbnail?.let { ThumbnailLoader.load(httpClient, driveToken, imageFileId, it) }
    }
}

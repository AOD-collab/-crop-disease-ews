package com.cropdisease.app

import android.Manifest
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.content.pm.ServiceInfo
import android.os.Build
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.ExistingWorkPolicy
import androidx.work.ForegroundInfo
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequest
import androidx.work.WorkManager
import androidx.work.Worker
import androidx.work.WorkerParameters
import okhttp3.OkHttpClient
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Uploads everything in the signed-in user's UploadQueue to Google Drive,
 * in the background (WorkManager), as a foreground job with an "Uploading
 * X of N" notification — so it keeps going when the app is closed, and
 * WorkManager restarts it after a reboot, when the connection comes back
 * (it only runs while online), or after a failure (with backoff).
 *
 * Only the transport moved here: each photo still becomes its own
 * /images/<email>/<image_id>.jpg on Drive, picked up by the laptop's Drive
 * watcher -> Kafka -> Spark exactly as before.
 */
class UploadWorker(context: Context, params: WorkerParameters) : Worker(context, params) {

    companion object {
        private const val UNIQUE_WORK = "photo-upload"
        private const val CHANNEL_ID = "uploads"
        private const val PROGRESS_NOTIFICATION_ID = 1001
        private const val DONE_NOTIFICATION_ID = 1002
        // Photos sent to Drive at the same time. Overlapping a few hides the
        // per-request round-trips on a slow link; more just splits the same
        // bandwidth and invites Drive rate limits.
        private const val PARALLELISM = 3

        /** Starts (or continues after) the upload of whatever is queued. */
        fun start(context: Context) = enqueue(context, ExistingWorkPolicy.APPEND_OR_REPLACE)

        /** Makes sure a queue left over from earlier (app killed, sign-in
         * expired, ...) is being worked on; does nothing if it already is. */
        fun resumeIfPending(context: Context) {
            val email = SessionStore(context).email ?: return
            Thread {
                if (UploadQueue.get(context).hasQueued(email)) enqueue(context, ExistingWorkPolicy.KEEP)
            }.start()
        }

        private fun enqueue(context: Context, policy: ExistingWorkPolicy) {
            val request = OneTimeWorkRequest.Builder(UploadWorker::class.java)
                .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .build()
            WorkManager.getInstance(context).enqueueUniqueWork(UNIQUE_WORK, policy, request)
        }
    }

    private val apiClient = OkHttpClient()
    // Photo uploads can take longer than a normal API call on a slow rural connection.
    private val uploadClient = apiClient.newBuilder()
        .writeTimeout(120, TimeUnit.SECONDS)
        .readTimeout(60, TimeUnit.SECONDS)
        .build()

    override fun doWork(): Result {
        val ctx = applicationContext
        val session = SessionStore(ctx)
        val email = session.email ?: return Result.success()  // signed out: rows wait for that user
        val queue = UploadQueue.get(ctx)
        if (!queue.hasQueued(email)) return Result.success()

        showProgress(queue.counts(email))
        val token = DriveAuth.accessTokenBlocking(ctx) ?: return signInNeeded(queue, email)
        val folders = try {
            session.folders(apiClient, token)
        } catch (e: Exception) {
            return Result.retry()  // offline / Drive hiccup
        } ?: run {
            notifyDone(ctx.getString(R.string.upload_done_no_access))
            return Result.failure()
        }

        val history = HistoryStore(ctx, email)
        val pool = Executors.newFixedThreadPool(PARALLELISM)
        try {
            while (!isStopped) {
                val batch = queue.nextQueued(email, PARALLELISM * 4)
                if (batch.isEmpty()) break
                val results = pool.invokeAll(batch.map { item ->
                    Callable { item to DriveUploader.upload(ctx, uploadClient, apiClient, item, folders) }
                }).map { it.get() }

                var transient = false
                var signIn = false
                val nowPending = mutableListOf<org.json.JSONObject>()
                for ((item, outcome) in results) when (outcome) {
                    is DriveUploader.Outcome.Uploaded -> {
                        queue.markUploaded(item, outcome.driveFileId)
                        nowPending.add(history.pendingItem(item.imageId, outcome.driveFileId, item.location))
                    }
                    is DriveUploader.Outcome.Rejected -> queue.markRejected(item, outcome.error)
                    is DriveUploader.Outcome.Failed -> queue.markFailed(item, outcome.error)
                    is DriveUploader.Outcome.Transient -> transient = true
                    DriveUploader.Outcome.SignInNeeded -> signIn = true
                }
                history.merge(nowPending)  // one save per round, not per photo
                showProgress(queue.counts(email))
                if (signIn) return signInNeeded(queue, email)
                if (transient) return Result.retry()  // WorkManager tries again later, rows stay queued
            }
        } finally {
            pool.shutdownNow()
        }
        if (isStopped) return Result.retry()

        val counts = queue.counts(email)
        notifyDone(
            if (counts.failed + counts.rejected == 0)
                ctx.getString(R.string.upload_done_all, counts.uploaded)
            else
                ctx.getString(R.string.upload_done_some_failed, counts.uploaded, counts.failed + counts.rejected)
        )
        return Result.success()
    }

    /** Leaves the photos queued: opening the app after signing in resumes them. */
    private fun signInNeeded(queue: UploadQueue, email: String): Result {
        notifyDone(applicationContext.getString(R.string.upload_paused_sign_in, queue.counts(email).queued))
        return Result.failure()
    }

    // ---- Notifications ----

    private fun ensureChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val manager = applicationContext.getSystemService(NotificationManager::class.java)
            manager.createNotificationChannel(NotificationChannel(
                CHANNEL_ID, applicationContext.getString(R.string.upload_channel_name), NotificationManager.IMPORTANCE_LOW
            ))
        }
    }

    private fun openAppIntent(): PendingIntent = PendingIntent.getActivity(
        applicationContext, 0,
        Intent(applicationContext, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP),
        PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,
    )

    /** Runs the job in the foreground with an "Uploading X of N" notification.
     * If Android won't allow that right now, the upload simply continues as
     * normal background work. */
    private fun showProgress(counts: UploadQueue.Counts) {
        ensureChannel()
        val notification = NotificationCompat.Builder(applicationContext, CHANNEL_ID)
            .setSmallIcon(android.R.drawable.stat_sys_upload)
            .setContentTitle(applicationContext.getString(R.string.upload_notification_title))
            .setContentText(applicationContext.getString(R.string.upload_progress, counts.done, counts.total))
            .setProgress(counts.total, counts.done, false)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setContentIntent(openAppIntent())
            .build()
        val info = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q)
            ForegroundInfo(PROGRESS_NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        else
            ForegroundInfo(PROGRESS_NOTIFICATION_ID, notification)
        try {
            setForegroundAsync(info).get()
        } catch (e: Exception) {
            // not allowed from the background right now — keep going without it
        }
    }

    private fun notifyDone(text: String) {
        val ctx = applicationContext
        if (Build.VERSION.SDK_INT >= 33 &&
            ContextCompat.checkSelfPermission(ctx, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED
        ) return
        ensureChannel()
        NotificationManagerCompat.from(ctx).notify(
            DONE_NOTIFICATION_ID,
            NotificationCompat.Builder(ctx, CHANNEL_ID)
                .setSmallIcon(android.R.drawable.stat_sys_upload_done)
                .setContentTitle(ctx.getString(R.string.upload_notification_title))
                .setContentText(text)
                .setStyle(NotificationCompat.BigTextStyle().bigText(text))
                .setAutoCancel(true)
                .setContentIntent(openAppIntent())
                .build()
        )
    }
}

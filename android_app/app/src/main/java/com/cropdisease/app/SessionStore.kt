package com.cropdisease.app

import android.content.Context

/**
 * Minimal app-private storage for who is signed in: the Google account's
 * email (the user's identity in the whole system — the laptop's Drive
 * watcher sees the same email as the owner of every uploaded photo) and a
 * display name. No password and no access token are stored: Google Play
 * services keeps the OAuth token and DriveAuth asks for a fresh one when
 * needed.
 *
 * `username` and `role` keep their old names so HomeActivity /
 * SettingsActivity show them exactly as before.
 */
class SessionStore(context: Context) {
    private val prefs = context.getSharedPreferences("session", Context.MODE_PRIVATE)

    var email: String?
        get() = prefs.getString("email", null)
        set(value) = prefs.edit().putString("email", value).apply()

    var username: String?
        get() = prefs.getString("username", null)
        set(value) = prefs.edit().putString("username", value).apply()

    var role: String?
        get() = prefs.getString("role", null)
        set(value) = prefs.edit().putString("role", value).apply()

    // This farmer's private Drive folders (/images/<email>/, /results/<email>/),
    // looked up once after sign-in by DriveRest.myFolders — see folders().
    var imagesFolderId: String?
        get() = prefs.getString("images_folder_id", null)
        set(value) = prefs.edit().putString("images_folder_id", value).apply()

    var resultsFolderId: String?
        get() = prefs.getString("results_folder_id", null)
        set(value) = prefs.edit().putString("results_folder_id", value).apply()

    val isLoggedIn: Boolean
        get() = !email.isNullOrEmpty()

    /**
     * The farmer's private folders, from the cache or looked up on Google
     * Drive (blocking — background thread only). Null if the administrator
     * hasn't shared them with this Google account yet.
     */
    fun folders(client: okhttp3.OkHttpClient, token: String): DriveRest.Folders? {
        val images = imagesFolderId
        val results = resultsFolderId
        if (images != null && results != null) return DriveRest.Folders(images, results)
        val found = DriveRest.myFolders(client, token, email.orEmpty()) ?: return null
        imagesFolderId = found.imagesId
        resultsFolderId = found.resultsId
        return found
    }

    fun clearSession() {
        prefs.edit().remove("email").remove("username").remove("role").remove("token")
            .remove("images_folder_id").remove("results_folder_id").apply()
    }
}

package com.cropdisease.app

import android.content.Context
import com.google.android.gms.auth.api.identity.AuthorizationRequest
import com.google.android.gms.auth.api.identity.Identity
import com.google.android.gms.common.api.Scope
import com.google.android.gms.tasks.Tasks

/**
 * OAuth access to Google Drive for the signed-in farmer, using Google
 * Identity Services' AuthorizationClient (the current Android API for
 * requesting OAuth scopes).
 *
 * The phone never stores a Google password or any key. Google gives the
 * app a short-lived access token for the user's OWN account; the central
 * account's folders are reachable because the administrator shared them
 * with that account (scripts/drive_share_user.py).
 *
 * Scope: full Drive, because the app must write into — and read results
 * from — folders owned by the central account, which the narrower
 * drive.file scope cannot see. Fine for an academic project while the
 * Google Cloud app is in Testing with listed test users (see DRIVE_SETUP.md).
 */
object DriveAuth {
    const val DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"

    fun authorizationRequest(): AuthorizationRequest =
        AuthorizationRequest.builder()
            .setRequestedScopes(listOf(Scope(DRIVE_SCOPE)))
            .build()

    /**
     * A fresh access token for the already-authorized account, or null if
     * the user has to sign in / consent again (then go to LoginActivity).
     * Blocking: call from a background thread only. Google caches and
     * refreshes the token, so calling this before each batch is cheap.
     */
    fun accessTokenBlocking(context: Context): String? = try {
        val result = Tasks.await(
            Identity.getAuthorizationClient(context).authorize(authorizationRequest())
        )
        if (result.hasResolution()) null else result.accessToken
    } catch (e: Exception) {
        null
    }
}

package com.cropdisease.app

import android.content.Intent
import android.os.Bundle
import android.view.View
import androidx.activity.result.IntentSenderRequest
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import com.cropdisease.app.databinding.ActivityLoginBinding
import com.google.android.gms.auth.api.identity.Identity
import okhttp3.OkHttpClient

/**
 * Sign-in screen: "Sign in with Google" (Google Identity Services
 * AuthorizationClient, requesting Drive access). Replaces the old
 * email/password login against our REST API, because the app now talks
 * only to Google Drive — there is no server of ours to check a password.
 *
 * The first time, Google shows its own account picker + consent screen;
 * after that, sign-in is silent. The account's email becomes the user's
 * identity (SessionStore.email). Access to the central folders is granted
 * by the administrator sharing them with this Google account.
 */
class LoginActivity : AppCompatActivity() {

    private lateinit var binding: ActivityLoginBinding
    private lateinit var session: SessionStore
    private val httpClient = OkHttpClient()

    /** Google's consent / account-picker screen, when it's needed. */
    private val consentLauncher = registerForActivityResult(
        ActivityResultContracts.StartIntentSenderForResult()
    ) { activityResult ->
        if (activityResult.resultCode != RESULT_OK) {
            setLoading(false)
            showError(getString(R.string.error_google_sign_in_cancelled))
            return@registerForActivityResult
        }
        try {
            val result = Identity.getAuthorizationClient(this)
                .getAuthorizationResultFromIntent(activityResult.data)
            onAuthorized(result.accessToken)
        } catch (e: Exception) {
            setLoading(false)
            showError(getString(R.string.error_google_sign_in_failed, e.message))
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityLoginBinding.inflate(layoutInflater)
        setContentView(binding.root)
        session = SessionStore(this)

        // Already signed in? Skip straight to Home. (If Google access was
        // revoked, the first Drive call there sends the user back here.)
        if (session.isLoggedIn) {
            goToHome()
            return
        }

        binding.loginButton.setOnClickListener { onSignInClicked() }
    }

    private fun onSignInClicked() {
        setLoading(true)
        showError("")
        Identity.getAuthorizationClient(this)
            .authorize(DriveAuth.authorizationRequest())
            .addOnSuccessListener { result ->
                val pendingIntent = result.pendingIntent
                if (result.hasResolution() && pendingIntent != null) {
                    consentLauncher.launch(IntentSenderRequest.Builder(pendingIntent.intentSender).build())
                } else {
                    onAuthorized(result.accessToken)
                }
            }
            .addOnFailureListener { e ->
                setLoading(false)
                showError(getString(R.string.error_google_sign_in_failed, e.message))
            }
    }

    /** Looks up which Google account was chosen, then opens Home. */
    private fun onAuthorized(accessToken: String?) {
        if (accessToken == null) {
            setLoading(false)
            showError(getString(R.string.error_google_sign_in_failed, "no access token"))
            return
        }
        Thread {
            val me = try {
                DriveRest.aboutMe(httpClient, accessToken)
            } catch (e: Exception) {
                runOnUiThread {
                    setLoading(false)
                    showError(getString(R.string.error_google_sign_in_failed, e.message))
                }
                return@Thread
            }
            runOnUiThread {
                setLoading(false)
                session.email = me.email
                session.username = me.displayName ?: me.email
                session.role = "USER"
                goToHome()
            }
        }.start()
    }

    private fun goToHome() {
        startActivity(Intent(this, HomeActivity::class.java))
        finish()
    }

    private fun setLoading(loading: Boolean) {
        binding.loginProgressBar.visibility = if (loading) View.VISIBLE else View.GONE
        binding.loginButton.isEnabled = !loading
    }

    private fun showError(message: String) {
        binding.loginStatusText.text = message
    }
}

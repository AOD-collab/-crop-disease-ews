package com.cropdisease.app

import android.content.Intent
import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import androidx.appcompat.app.AppCompatDelegate
import androidx.core.os.LocaleListCompat
import com.cropdisease.app.databinding.ActivitySettingsBinding

/**
 * Account info + log out — the third bottom-nav tab. Deliberately minimal
 * and offline: it just reflects what SessionStore already holds and lets
 * the user sign out, no network calls of its own.
 *
 * Also hosts the app's language switch. The phone's own system language
 * list has no Tetum option on any stock/OEM Android build, so relying on
 * the system locale (values-tet/) alone would never actually show Tetum
 * text to a real user — this per-app override (AppCompatDelegate,
 * supported since appcompat 1.6+) lets the app force itself into Tetum
 * regardless of what the phone's system settings offer, and AppCompat
 * auto-recreates every AppCompatActivity to apply it immediately.
 */
class SettingsActivity : AppCompatActivity() {

    private lateinit var binding: ActivitySettingsBinding
    private lateinit var session: SessionStore

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        session = SessionStore(this)

        if (!session.isLoggedIn) {
            startActivity(Intent(this, LoginActivity::class.java))
            finish()
            return
        }

        binding = ActivitySettingsBinding.inflate(layoutInflater)
        setContentView(binding.root)

        binding.accountNameText.text = session.username ?: getString(R.string.dash_placeholder)
        binding.accountRoleText.text = session.role ?: getString(R.string.dash_placeholder)
        binding.logoutButton.setOnClickListener { logout() }

        binding.languageEnglishButton.setOnClickListener { setAppLanguage("en") }
        binding.languageTetumButton.setOnClickListener { setAppLanguage("tet") }
        updateLanguageButtons()

        BottomNav.setup(this, binding.bottomNav, BottomNav.Tab.SETTINGS)
    }

    private fun setAppLanguage(languageTag: String) {
        AppCompatDelegate.setApplicationLocales(LocaleListCompat.forLanguageTags(languageTag))
    }

    /** Disables whichever button matches the currently active app locale,
     * so the two buttons double as a status indicator, not just a switch. */
    private fun updateLanguageButtons() {
        val current = AppCompatDelegate.getApplicationLocales()
        val isTetum = !current.isEmpty && current[0]?.language == "tet"
        binding.languageTetumButton.isEnabled = !isTetum
        binding.languageEnglishButton.isEnabled = isTetum
    }

    private fun logout() {
        session.clearSession()
        val intent = Intent(this, LoginActivity::class.java)
        intent.flags = Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_NEW_TASK
        startActivity(intent)
        finish()
    }
}

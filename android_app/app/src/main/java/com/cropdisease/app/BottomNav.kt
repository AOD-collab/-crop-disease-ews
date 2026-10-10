package com.cropdisease.app

import android.content.Intent
import androidx.appcompat.app.AppCompatActivity
import com.google.android.material.bottomnavigation.BottomNavigationView

/**
 * Wires the same three-tab bottom nav (Home / History / Settings) into
 * whichever of those Activities is currently showing. Kept flat rather than
 * Fragment-based (consistent with the rest of this app's plain
 * multi-Activity structure) — switching tabs finishes the current activity
 * and starts the target fresh, so there's never more than one tab activity
 * alive and the back stack never grows across tabs.
 */
object BottomNav {

    enum class Tab { HOME, HISTORY, SETTINGS }

    fun setup(activity: AppCompatActivity, bottomNav: BottomNavigationView, current: Tab) {
        bottomNav.selectedItemId = when (current) {
            Tab.HOME -> R.id.nav_home
            Tab.HISTORY -> R.id.nav_history
            Tab.SETTINGS -> R.id.nav_settings
        }
        bottomNav.setOnItemSelectedListener { item ->
            val target: Class<out AppCompatActivity>? = when (item.itemId) {
                R.id.nav_home -> if (current != Tab.HOME) HomeActivity::class.java else null
                R.id.nav_history -> if (current != Tab.HISTORY) HistoryActivity::class.java else null
                R.id.nav_settings -> if (current != Tab.SETTINGS) SettingsActivity::class.java else null
                else -> null
            }
            if (target != null) {
                val intent = Intent(activity, target)
                intent.flags = Intent.FLAG_ACTIVITY_CLEAR_TOP or Intent.FLAG_ACTIVITY_SINGLE_TOP
                activity.startActivity(intent)
                activity.finish()
            }
            true
        }
    }
}

package com.cropdisease.app

import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone

/**
 * Shows server/Drive timestamps (ISO-8601 in UTC, e.g. Drive's
 * "2026-09-28T03:21:48.261Z" or Python's "2026-09-28T03:21:48+00:00") in
 * Timor-Leste Time (Asia/Dili, UTC+9, no daylight saving), as
 * "28/09/2026 12:21". Same format as the admin dashboard
 * (config/localtime.py). SimpleDateFormat, not java.time: minSdk is 24.
 */
object TimorTime {
    private val TIMOR = TimeZone.getTimeZone("Asia/Dili")
    private val ISO = Regex("""^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.\d+)?(Z|[+-]\d{2}:?\d{2})?$""")

    /** "" for blank input; the raw string back if it can't be parsed. */
    fun format(iso: String?, withTime: Boolean = true): String {
        if (iso.isNullOrBlank()) return ""
        val m = ISO.matchEntire(iso.trim()) ?: return iso
        val (date, time, zone) = m.destructured
        val parser = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ", Locale.US)
        val offset = when {
            zone.isEmpty() || zone == "Z" -> "+0000"   // no zone: stored as UTC
            else -> zone.replace(":", "")
        }
        val instant = try {
            parser.parse("${date}T$time$offset")
        } catch (e: Exception) {
            null
        } ?: return iso
        val out = SimpleDateFormat(if (withTime) "dd/MM/yyyy HH:mm" else "dd/MM/yyyy", Locale.US)
        out.timeZone = TIMOR
        return out.format(instant)
    }
}

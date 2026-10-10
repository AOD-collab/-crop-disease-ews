package com.cropdisease.app

import android.content.Context
import android.content.res.ColorStateList
import android.view.Gravity
import android.view.View
import android.widget.ImageView
import android.widget.LinearLayout
import android.widget.TextView
import androidx.core.content.ContextCompat

/**
 * Shared "outcome row" look for the two screens that list per-image upload
 * outcomes as colored rows — MainActivity's batch upload results and
 * HistoryActivity's upload history — so both render identically instead of
 * drifting apart as separate hand-rolled layouts.
 *
 * Colors follow the app's status palette (see colors.xml): "healthy" and
 * "diseased" are verdicts (good/critical), "failed" is a pipeline problem,
 * not a verdict about the crop (serious), and anything still in flight
 * ("pending", or any other in-progress label) stays neutral — a state
 * with no verdict yet shouldn't read as alarming.
 */
object OutcomeUi {

    /** [thumbnail] is the (original) photo slot; [processed] is only set by
     * addComparisonRow, for the marked-up image shown next to it. */
    data class Row(
        val container: LinearLayout, val label: TextView, val accent: View, val thumbnail: ImageView?,
        val processed: ImageView? = null,
    )

    fun colorFor(context: Context, outcome: String): Int = ContextCompat.getColor(
        context,
        when (outcome) {
            "healthy" -> R.color.status_good
            "diseased" -> R.color.status_critical
            "failed" -> R.color.status_serious
            else -> R.color.text_muted // pending / in-progress
        }
    )

    /** Appends one bordered, left-accented row to [container] and returns
     * its parts so the caller can update the text and/or accent color in
     * place later (e.g. resolving a "pending" row to its final outcome).
     * Pass [withThumbnail] = true to also reserve a square image slot
     * (caller fills it in — see ThumbnailLoader — since OutcomeUi has no
     * network/auth context of its own). */
    fun addRow(
        context: Context, container: LinearLayout, text: String, accentColor: Int,
        withThumbnail: Boolean = false,
    ): Row {
        val density = context.resources.displayMetrics.density
        fun dp(v: Int) = (v * density).toInt()

        val row = LinearLayout(context).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
            background = ContextCompat.getDrawable(context, R.drawable.bg_list_row)
            setPadding(dp(12), dp(12), dp(12), dp(12))
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = dp(8) }
        }

        val accent = View(context).apply {
            layoutParams = LinearLayout.LayoutParams(dp(4), dp(36))
            background = ContextCompat.getDrawable(context, R.drawable.bg_accent_bar)
            backgroundTintList = ColorStateList.valueOf(accentColor)
        }
        row.addView(accent)

        val thumbnail = if (withThumbnail) {
            ImageView(context).apply {
                layoutParams = LinearLayout.LayoutParams(dp(44), dp(44)).apply { marginStart = dp(10) }
                scaleType = ImageView.ScaleType.CENTER_CROP
                clipToOutline = true
                background = ContextCompat.getDrawable(context, R.drawable.bg_thumb_placeholder)
                contentDescription = null
            }.also { row.addView(it) }
        } else null

        val label = TextView(context).apply {
            this.text = text
            textSize = 14f
            setTextColor(ContextCompat.getColor(context, R.color.text_primary))
            layoutParams = LinearLayout.LayoutParams(
                0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f
            ).apply { marginStart = dp(12) }
        }
        row.addView(label)

        container.addView(row)
        return Row(row, label, accent, thumbnail)
    }

    /**
     * A result row with the ORIGINAL photo and the PROCESSED (marked-up)
     * photo side by side under the status text — used for the results shown
     * right after an upload. The caller fills [Row.thumbnail] (original) and,
     * once Spark has finished, [Row.processed].
     */
    fun addComparisonRow(context: Context, container: LinearLayout, text: String, accentColor: Int): Row {
        val density = context.resources.displayMetrics.density
        fun dp(v: Int) = (v * density).toInt()

        val outer = LinearLayout(context).apply {
            orientation = LinearLayout.VERTICAL
            background = ContextCompat.getDrawable(context, R.drawable.bg_list_row)
            setPadding(dp(12), dp(12), dp(12), dp(12))
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = dp(8) }
        }

        val header = LinearLayout(context).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
        }
        val accent = View(context).apply {
            layoutParams = LinearLayout.LayoutParams(dp(4), dp(36))
            background = ContextCompat.getDrawable(context, R.drawable.bg_accent_bar)
            backgroundTintList = ColorStateList.valueOf(accentColor)
        }
        header.addView(accent)
        val label = TextView(context).apply {
            this.text = text
            textSize = 14f
            setTextColor(ContextCompat.getColor(context, R.color.text_primary))
            layoutParams = LinearLayout.LayoutParams(
                0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f
            ).apply { marginStart = dp(12) }
        }
        header.addView(label)
        outer.addView(header)

        val images = LinearLayout(context).apply {
            orientation = LinearLayout.HORIZONTAL
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = dp(10) }
        }
        fun imageColumn(caption: String, startMargin: Int): ImageView {
            val column = LinearLayout(context).apply {
                orientation = LinearLayout.VERTICAL
                layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
                    .apply { marginStart = startMargin }
            }
            column.addView(TextView(context).apply {
                this.text = caption
                textSize = 12f
                setTextColor(ContextCompat.getColor(context, R.color.text_secondary))
            })
            val image = ImageView(context).apply {
                layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, dp(150))
                    .apply { topMargin = dp(4) }
                scaleType = ImageView.ScaleType.FIT_CENTER
                adjustViewBounds = true
                background = ContextCompat.getDrawable(context, R.drawable.bg_thumb_placeholder)
                contentDescription = caption
            }
            column.addView(image)
            images.addView(column)
            return image
        }
        val original = imageColumn(context.getString(R.string.label_original_photo), 0)
        val processed = imageColumn(context.getString(R.string.label_processed_photo), dp(8))
        outer.addView(images)

        container.addView(outer)
        return Row(outer, label, accent, original, processed)
    }

    fun setAccent(row: Row, color: Int) {
        row.accent.backgroundTintList = ColorStateList.valueOf(color)
    }
}

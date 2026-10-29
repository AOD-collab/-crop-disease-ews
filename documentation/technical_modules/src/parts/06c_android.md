
### D.2 `UploadQueue.kt` — fila upload iha telemóvel

*Nota:* bloku Kotlin; pasu 3 esplika fundamentu Kotlin (tabela A.3).

#### D.2.1 Header, singleton, `Item`, `Counts`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/UploadQueue.kt 1-71}}

**2. Objetivu**

Fila fotografia ba upload, rai iha telemóvel (SQLite privadu) atu upload rihun moris liu app taka, telemóvel restart ka rede monu. Kada liña hetan `image_id` bainhira tama fila no mantein iha kada retry. Define estadu 4, singleton thread-safe, no klase dadus `Item` no `Counts`.

**3. Fundamentu Python**

*(Kotlin)* `class UploadQueue private constructor(...) : SQLiteOpenHelper(context, "upload_queue.db", null, 1)` — herança + konstrutór privadu; `companion object` — membru klase; `@Volatile` + `synchronized` — singleton thread-safe; `?:` + `.also { }`; `val done get() = total - queued` — propriedade kalkula.

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `package com.cropdisease.app`.
- **Liña 3** — `import android.content.ContentValues`.
- **Liña 4** — `import android.content.Context`.
- **Liña 5** — `import android.content.Intent` — flag permisaun.
- **Liña 6** — `import android.database.Cursor`.
- **Liña 7** — `import android.database.sqlite.SQLiteDatabase`.
- **Liña 8** — `import android.database.sqlite.SQLiteOpenHelper`.
- **Liña 9** — `import android.net.Uri`.
- **Liña 10** — `import java.io.File`.
- **Liña 12–30** — KDoc: fotografia hein upload ba Google Drive, rai iha telemóvel (SQLite ki'ik iha armazenamentu privadu app), atu upload fotografia rihun moris liu app taka, telemóvel restart ka rede monu; `UploadWorker` serbisu iha background; ekran upload só aumenta no hatudu progresu. Kada liña hetan `image_id` bainhira tama no mantein iha kada retry — `DriveUploader` buka `<image_id>.jpg` iha Drive molok haruka fali, entaun fotografia nunka iha Drive (no Spark) dala rua. Fotografia LA kopia bainhira app bele mantein asesu lee (permisaun uri persiste, ka pasta "Choose folder"); só fotografia kámara no fotografia galeria ne'ebé asesu labele mantein rai iha `filesDir/upload_queue/` — hamoos depois upload. Liña kada konta Google (`user_email`): worker só upload liña utilizadór ne'ebé login, ho nia token, ba nia pasta.
- **Liña 31–32** — `class UploadQueue private constructor(private val context: Context) : SQLiteOpenHelper(context, "upload_queue.db", null, 1) {` — base de dadus `upload_queue.db` versaun 1.
- **Liña 34** — `companion object {`
- **Liña 35** — `const val QUEUED = "QUEUED"`.
- **Liña 36** — `const val UPLOADED = "UPLOADED"`.
- **Liña 37** — `const val FAILED = "FAILED"` — komentáriu: bele koko fali ("Retry failed").
- **Liña 38** — `const val REJECTED = "REJECTED"` — komentáriu: la'ós fotografia útil — koko fali la ajuda.
- **Liña 40** — `@Volatile private var instance: UploadQueue? = null` — thread hotu haree valór foun.
- **Liña 42** — `fun get(context: Context): UploadQueue = instance ?: synchronized(this) {` — se iha ona, fó; se lae, tama lock.
- **Liña 43** — `instance ?: UploadQueue(context.applicationContext).also { instance = it }` — kontrola fali, harii, rai.
- **Liña 44** — `}`.
- **Liña 46** — KDoc: fatin kópia fila (fotografia kámara, galeria la bele kopia).
- **Liña 47** — `fun ownedDir(context: Context) = File(context.filesDir, "upload_queue")`.
- **Liña 48** — `}` — remata companion.
- **Liña 50** — `data class Item(`
- **Liña 51** — `val imageId: String,` — UUID.
- **Liña 52** — `val userEmail: String,`
- **Liña 53** — `val uri: String,` — enderesu fotografia.
- **Liña 54** — `val displayName: String,`
- **Liña 55** — `val mimeType: String,`
- **Liña 56** — `val location: String?,`
- **Liña 57** — `val gpsLat: Double? = null,`
- **Liña 58** — `val gpsLon: Double? = null,`
- **Liña 59** — `val gpsAccM: Float? = null,`
- **Liña 60** — KDoc: uri mak kópia rasik iha `ownedDir` — hamoos depois upload.
- **Liña 61** — `val ownedFile: Boolean = false,`
- **Liña 62** — KDoc: pasta ("Choose folder") ne'ebé fotografia ne'e lee liuhusi.
- **Liña 63** — `val treeUri: String? = null,`
- **Liña 64** — `val state: String = QUEUED,`
- **Liña 65** — `val error: String? = null,`
- **Liña 66** — `val driveFileId: String? = null,`
- **Liña 67** — `)`.
- **Liña 69** — `data class Counts(val total: Int, val queued: Int, val uploaded: Int, val failed: Int, val rejected: Int) {`
- **Liña 70** — `val done get() = total - queued` — "X of N" iha notifikasaun.
- **Liña 71** — `}`.

**5. Walkthrough ezekusaun**

`UploadQueue.get(ctx)` dala uluk → harii objetu → rai iha `instance`; dala tuirmai → objetu hanesan. `Counts(total=10, queued=3, ...)` → `done = 7`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `QUEUED`... | `String` (`const`) |
| `instance` | `UploadQueue?` |
| `Item.*` | `String`, `String?`, `Double?`, `Float?`, `Boolean` |
| `Counts.done` | `Int` |

**7. Ligasaun OOP**

- **Herança**: `UploadQueue : SQLiteOpenHelper` (klase Android).
- **Singleton** ho konstrutór privadu + `companion object.get` + double-checked locking.
- **Data class** `Item`, `Counts` (valór imutável).
- **Enkapsulamentu**: `private val context`, konstrutór privadu.

**8. Impaktu integrasaun**

`UploadWorker` lee fila; `DriveUploader.upload` simu `Item`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `image_id` harii bainhira tama fila (la'ós bainhira upload)?
**R:** Atu retry hotu uza ID hanesan; `findImage` hetan fotografia ne'ebé upload ona → la iha duplikadu iha Drive no Spark.

#### D.2.2 Tabela SQLite, `enqueue`, `nextQueued`, `hasQueued`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/UploadQueue.kt 73-135}}

**2. Objetivu**

Harii tabela `queue` + índise; aumenta fotografia ba fila iha transasaun (ignora `image_id` repete); se la iha buat ida hein, hamoos liña tuan uluk (progresu "X of N" foun); foti fotografia hein tuir orden; kontrola se iha fotografia hein.

**3. Fundamentu Python**

*(Kotlin)* `override fun onCreate(db: SQLiteDatabase)`; string multi-liña `"""..."""`; `db.insertWithOnConflict(..., CONFLICT_IGNORE)`; `try { } finally { db.endTransaction() }`; `rawQuery(...).use { it.moveToFirst() }`.

**4. Esplikasaun liña-ba-liña**

- **Liña 73** — `override fun onCreate(db: SQLiteDatabase) {` — Android bolu bainhira base de dadus foun.
- **Liña 74** — `db.execSQL(`
- **Liña 75** — `"""CREATE TABLE queue (`
- **Liña 76** — `image_id TEXT PRIMARY KEY,`
- **Liña 77** — `user_email TEXT NOT NULL,`
- **Liña 78** — `uri TEXT NOT NULL,`
- **Liña 79** — `display_name TEXT NOT NULL,`
- **Liña 80** — `mime_type TEXT NOT NULL,`
- **Liña 81** — `location TEXT,`
- **Liña 82** — `gps_lat REAL, gps_lon REAL, gps_acc REAL,`
- **Liña 83** — `owned_file INTEGER NOT NULL DEFAULT 0,`
- **Liña 84** — `tree_uri TEXT,`
- **Liña 85** — `state TEXT NOT NULL,`
- **Liña 86** — `error TEXT,`
- **Liña 87** — `drive_file_id TEXT`
- **Liña 88** — `)"""`.
- **Liña 89** — `)`.
- **Liña 90** — `db.execSQL("CREATE INDEX queue_user_state ON queue(user_email, state)")`.
- **Liña 91** — `}`.
- **Liña 93** — `override fun onUpgrade(db: SQLiteDatabase, oldVersion: Int, newVersion: Int) {}` — versaun 1 de'it (la iha migrasaun).
- **Liña 95–99** — KDoc: aumenta fotografia; se la iha buat hein ba utilizadór ne'e, hamoos liña upload tuan (remata) uluk, entaun progresu "X of N" ba fotografia desde fila mamuk ikus.
- **Liña 100** — `fun enqueue(items: List<Item>) {`
- **Liña 101** — `if (items.isEmpty()) return`.
- **Liña 102** — `val email = items.first().userEmail`.
- **Liña 103** — `val db = writableDatabase`.
- **Liña 104** — `if (!hasQueued(email)) clearFinished(email)`.
- **Liña 105** — `db.beginTransaction()`.
- **Liña 106** — `try {`
- **Liña 107** — `for (item in items) {`
- **Liña 108** — `db.insertWithOnConflict("queue", null, ContentValues().apply {`
- **Liña 109** — `put("image_id", item.imageId)`.
- **Liña 110** — `put("user_email", item.userEmail)`.
- **Liña 111** — `put("uri", item.uri)`.
- **Liña 112** — `put("display_name", item.displayName)`.
- **Liña 113** — `put("mime_type", item.mimeType)`.
- **Liña 114** — `put("location", item.location)`.
- **Liña 115** — `item.gpsLat?.let { put("gps_lat", it) }`.
- **Liña 116** — `item.gpsLon?.let { put("gps_lon", it) }`.
- **Liña 117** — `item.gpsAccM?.let { put("gps_acc", it) }`.
- **Liña 118** — `put("owned_file", if (item.ownedFile) 1 else 0)`.
- **Liña 119** — `put("tree_uri", item.treeUri)`.
- **Liña 120** — `put("state", QUEUED)`.
- **Liña 121** — `}, SQLiteDatabase.CONFLICT_IGNORE)` — `image_id` repete → ignora.
- **Liña 122** — `}`.
- **Liña 123** — `db.setTransactionSuccessful()`.
- **Liña 124** — `} finally {`
- **Liña 125** — `db.endTransaction()` — commit (se susesu) ka rollback.
- **Liña 126** — `}`.
- **Liña 127** — `}`.
- **Liña 129** — `fun nextQueued(email: String, limit: Int): List<Item> = query(`
- **Liña 130** — `"user_email = ? AND state = ?", arrayOf(email, QUEUED), limit` — parámetru `?` (seguru).
- **Liña 131** — `)`.
- **Liña 133** — `fun hasQueued(email: String): Boolean = readableDatabase.rawQuery(`
- **Liña 134** — `"SELECT 1 FROM queue WHERE user_email = ? AND state = ? LIMIT 1", arrayOf(email, QUEUED)`.
- **Liña 135** — `).use { it.moveToFirst() }` — `true` se iha liña.

**5. Walkthrough ezekusaun**

Agrikultór hili fotografia 50 → `enqueue(50 Item)` → transasaun ida → liña 50 `QUEUED`. `nextQueued(email, 12)` → 12 dahuluk tuir `rowid`.

**6. Tipu dadus**

`enqueue(List<Item>)` → `Unit`; `nextQueued` → `List<Item>`; `hasQueued` → `Boolean`.

**7. Ligasaun OOP**

**Polimorfizmu (override)**: `onCreate`, `onUpgrade` substitui métodu abstratu `SQLiteOpenHelper`. `writableDatabase`/`readableDatabase` — propriedade herda.

**8. Impaktu integrasaun**

Ekran upload bolu `enqueue` no `UploadWorker.start`.

**9. Checkpoint ba principiante**

**P:** Tanba sá transasaun iha `enqueue`?
**R:** Liña hotu tama hamutuk (lalais no konsistente); se erru, la iha liña sorin-balun.

#### D.2.3 `usesTree`, `counts`, `items`, `mark*`, `retryFailed`, `clearFinished`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/UploadQueue.kt 137-191}}

**2. Objetivu**

Kontrola pasta sei uza; konta liña tuir estadu (progresu); foti liña tuir `image_id`; marka UPLOADED/REJECTED/FAILED; tau fali FAILED ba fila; hamoos liña remata.

**3. Fundamentu Python**

*(Kotlin)* `GROUP BY` + `while (c.moveToNext())`; `byState[QUEUED] ?: 0`; `chunked(500)`; `joinToString(",") { "?" }`.

**4. Esplikasaun liña-ba-liña**

- **Liña 137** — KDoc: loos bainhira fotografia hein (ka bele koko fali) sei lee liuhusi pasta ne'e.
- **Liña 138** — `fun usesTree(treeUri: String): Boolean = readableDatabase.rawQuery(`
- **Liña 139** — `"SELECT 1 FROM queue WHERE tree_uri = ? AND state IN (?, ?) LIMIT 1", arrayOf(treeUri, QUEUED, FAILED)`.
- **Liña 140** — `).use { it.moveToFirst() }`.
- **Liña 142** — `fun counts(email: String): Counts {`
- **Liña 143** — `val byState = mutableMapOf<String, Int>()`.
- **Liña 144** — `readableDatabase.rawQuery(`
- **Liña 145** — `"SELECT state, COUNT(*) FROM queue WHERE user_email = ? GROUP BY state", arrayOf(email)`.
- **Liña 146** — `).use { c -> while (c.moveToNext()) byState[c.getString(0)] = c.getInt(1) }`.
- **Liña 147** — `return Counts(`
- **Liña 148** — `total = byState.values.sum(),`
- **Liña 149** — `queued = byState[QUEUED] ?: 0,`
- **Liña 150** — `uploaded = byState[UPLOADED] ?: 0,`
- **Liña 151** — `failed = byState[FAILED] ?: 0,`
- **Liña 152** — `rejected = byState[REJECTED] ?: 0,`
- **Liña 153** — `)`.
- **Liña 154** — `}`.
- **Liña 156** — KDoc: liña atuál ba `image_id` sira ne'e (ba liña rezultadu inline).
- **Liña 157** — `fun items(imageIds: Collection<String>): Map<String, Item> {`
- **Liña 158** — `if (imageIds.isEmpty()) return emptyMap()`.
- **Liña 159** — `val out = mutableMapOf<String, Item>()`.
- **Liña 160** — `for (chunk in imageIds.chunked(500)) {`
- **Liña 161** — `val marks = chunk.joinToString(",") { "?" }` — "?,?,?...".
- **Liña 162** — `for (item in query("image_id IN ($marks)", chunk.toTypedArray(), null)) out[item.imageId] = item`.
- **Liña 163** — `}`.
- **Liña 164** — `return out`.
- **Liña 165** — `}`.
- **Liña 167** — `fun markUploaded(item: Item, driveFileId: String) {`
- **Liña 168** — `update(item.imageId, UPLOADED, null, driveFileId)`.
- **Liña 169** — `releaseSource(item)` — liberta permisaun/kópia.
- **Liña 170** — `}`.
- **Liña 172** — `fun markRejected(item: Item, error: String) {`
- **Liña 173** — `update(item.imageId, REJECTED, error, null)`.
- **Liña 174** — `releaseSource(item)`.
- **Liña 175** — `}`.
- **Liña 177** — KDoc: mantein fonte fotografia, atu "Retry failed" bele haruka fali.
- **Liña 178** — `fun markFailed(item: Item, error: String) = update(item.imageId, FAILED, error, null)`.
- **Liña 180** — KDoc: tau fali fotografia FAILED hotu utilizadór ne'e ba fila; fó kuantu.
- **Liña 181** — `fun retryFailed(email: String): Int = writableDatabase.update(`
- **Liña 182** — `"queue", ContentValues().apply { put("state", QUEUED); putNull("error") },`
- **Liña 183** — `"user_email = ? AND state = ?", arrayOf(email, FAILED)`.
- **Liña 184** — `)`.
- **Liña 186** — KDoc: hasai liña remata (liberta buat ne'ebé liña FAILED kaer hela).
- **Liña 187** — `private fun clearFinished(email: String) {`
- **Liña 188** — `val failed = query("user_email = ? AND state = ?", arrayOf(email, FAILED), null)`.
- **Liña 189** — `writableDatabase.delete("queue", "user_email = ? AND state != ?", arrayOf(email, QUEUED))`.
- **Liña 190** — `for (item in failed) releaseSource(item)` — komentáriu: depois delete, atu pasta sira konta hanesan la uza.
- **Liña 191** — `}`.

**5. Walkthrough ezekusaun**

Upload 10: 8 UPLOADED, 1 FAILED, 1 REJECTED → `counts` = `Counts(10, 0, 8, 1, 1)` → notifikasaun "8 uploaded, 2 failed". "Retry failed" → `retryFailed` → 1 liña ba `QUEUED`.

**6. Tipu dadus**

`usesTree -> Boolean`; `counts -> Counts`; `items -> Map<String, Item>`; `retryFailed -> Int`.

**7. Ligasaun OOP**

Métodu publiku (API fila) vs `private` (`clearFinished`) — enkapsulamentu formál.

**8. Impaktu integrasaun**

`UploadWorker` bolu `markUploaded/Rejected/Failed`, `counts`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `markFailed` la bolu `releaseSource`?
**R:** Fotografia FAILED bele haruka fali ("Retry failed"); presiza mantein asesu ba fonte.

#### D.2.4 `releaseSource`, `update`, `query`, `Cursor.toItem`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/UploadQueue.kt 193-249}}

**2. Objetivu**

Liberta fonte fotografia bainhira la presiza (hamoos kópia rasik; liberta permisaun uri; liberta permisaun pasta bainhira la iha fotografia hein ne'ebé uza); atualiza estadu; query jenériku; konverte liña `Cursor` ba `Item`.

**3. Fundamentu Python**

*(Kotlin)* `when { kond -> ... }`; extension function `private fun Cursor.toItem()`; funsaun lokál `fun str(col: String) = ...`; `buildList { while (...) add(...) }`; `error?.take(300)`.

**4. Esplikasaun liña-ba-liña**

- **Liña 193–196** — KDoc: husik fotografia bainhira la presiza: kópia rasik hamoos, permisaun lee persiste liberta (Android fó app ida númeru limitadu), no permisaun pasta liberta bainhira fotografia hein la lee liuhusi nia.
- **Liña 197** — `private fun releaseSource(item: Item) {`
- **Liña 198** — `val uri = Uri.parse(item.uri)`.
- **Liña 199** — `try {`
- **Liña 200** — `when {`
- **Liña 201** — `item.ownedFile -> uri.path?.let { File(it).delete() }`.
- **Liña 202** — `item.treeUri != null -> {`
- **Liña 203–205** — `if (!usesTree(item.treeUri)) context.contentResolver.releasePersistableUriPermission(Uri.parse(item.treeUri), Intent.FLAG_GRANT_READ_URI_PERMISSION)`.
- **Liña 206** — `}`.
- **Liña 207–209** — `else -> context.contentResolver.releasePersistableUriPermission(uri, Intent.FLAG_GRANT_READ_URI_PERMISSION)`.
- **Liña 210** — `}`.
- **Liña 211** — `} catch (e: Exception) {`
- **Liña 212** — Komentáriu: la kaer buat ida (ezemplu permisaun nunka persiste) — di'ak.
- **Liña 213** — `}`.
- **Liña 214** — `}`.
- **Liña 216** — `private fun update(imageId: String, state: String, error: String?, driveFileId: String?) {`
- **Liña 217** — `writableDatabase.update("queue", ContentValues().apply {`
- **Liña 218** — `put("state", state)`.
- **Liña 219** — `put("error", error?.take(300))`.
- **Liña 220** — `if (driveFileId != null) put("drive_file_id", driveFileId)`.
- **Liña 221** — `}, "image_id = ?", arrayOf(imageId))`.
- **Liña 222** — `}`.
- **Liña 224** — `private fun query(where: String, args: Array<String>, limit: Int?): List<Item> =`
- **Liña 225** — `readableDatabase.query(`
- **Liña 226** — `"queue", null, where, args, null, null, "rowid", limit?.toString()` — orden tuir `rowid` (orden tama).
- **Liña 227** — `).use { c -> buildList { while (c.moveToNext()) add(c.toItem()) } }`.
- **Liña 229** — `private fun Cursor.toItem(): Item {`
- **Liña 230** — `fun str(col: String) = getColumnIndexOrThrow(col).let { if (isNull(it)) null else getString(it) }`.
- **Liña 231** — `fun dbl(col: String) = getColumnIndexOrThrow(col).let { if (isNull(it)) null else getDouble(it) }`.
- **Liña 232** — `return Item(`
- **Liña 233** — `imageId = str("image_id")!!,`
- **Liña 234** — `userEmail = str("user_email")!!,`
- **Liña 235** — `uri = str("uri")!!,`
- **Liña 236** — `displayName = str("display_name")!!,`
- **Liña 237** — `mimeType = str("mime_type")!!,`
- **Liña 238** — `location = str("location"),`
- **Liña 239** — `gpsLat = dbl("gps_lat"),`
- **Liña 240** — `gpsLon = dbl("gps_lon"),`
- **Liña 241** — `gpsAccM = dbl("gps_acc")?.toFloat(),`
- **Liña 242** — `ownedFile = getInt(getColumnIndexOrThrow("owned_file")) == 1,`
- **Liña 243** — `treeUri = str("tree_uri"),`
- **Liña 244** — `state = str("state")!!,`
- **Liña 245** — `error = str("error"),`
- **Liña 246** — `driveFileId = str("drive_file_id"),`
- **Liña 247** — `)`.
- **Liña 248** — `}`.
- **Liña 249** — `}` — remata klase.

**5. Walkthrough ezekusaun**

Fotografia kámara (`ownedFile = true`) UPLOADED → `releaseSource` → hamoos `filesDir/upload_queue/<id>.jpg`. Fotografia galeria → liberta permisaun uri.

**6. Tipu dadus**

`releaseSource -> Unit`; `update -> Unit`; `query -> List<Item>`; `Cursor.toItem -> Item`.

**7. Ligasaun OOP**

**Extension function** `Cursor.toItem()` — aumenta métodu ba klase Android `Cursor` la ho herda (Kotlin espesífiku). Métodu `private` — enkapsulamentu.

**8. Impaktu integrasaun**

Android limita númeru permisaun uri persiste ba app ida — liberta importante ba upload rihun.

**9. Checkpoint ba principiante**

**P:** Saida mak extension function?
**R:** Funsaun ne'ebé hakerek "hanesan" métodu klase seluk (`cursor.toItem()`) maske klase ne'e la iha nia. Python la iha ida ne'e (Teoria).

### D.3 `UploadWorker.kt` — upload background

#### D.3.1 Header, konstante, `start`, `resumeIfPending`, `enqueue`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/UploadWorker.kt 1-71}}

**2. Objetivu**

Upload fila utilizadór ba Drive iha background (WorkManager), hanesan serbisu foreground ho notifikasaun "Uploading X of N"; kontinua bainhira app taka; WorkManager hahú fali depois reboot, rede fila, ka falla (backoff). Define naran serbisu, kanál notifikasaun, paralelismu 3, no oinsá hahú.

**3. Fundamentu Python**

*(Kotlin)* `class UploadWorker(context: Context, params: WorkerParameters) : Worker(context, params)`; `companion object`; `Thread { ... }.start()`; Builder WorkManager (`Constraints`, `BackoffPolicy.EXPONENTIAL`); `ExistingWorkPolicy.APPEND_OR_REPLACE` / `KEEP`.

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `package com.cropdisease.app`.
- **Liña 3** — `import android.Manifest`.
- **Liña 4** — `import android.app.NotificationChannel`.
- **Liña 5** — `import android.app.NotificationManager`.
- **Liña 6** — `import android.app.PendingIntent`.
- **Liña 7** — `import android.content.Context`.
- **Liña 8** — `import android.content.Intent`.
- **Liña 9** — `import android.content.pm.PackageManager`.
- **Liña 10** — `import android.content.pm.ServiceInfo`.
- **Liña 11** — `import android.os.Build`.
- **Liña 12** — `import androidx.core.app.NotificationCompat`.
- **Liña 13** — `import androidx.core.app.NotificationManagerCompat`.
- **Liña 14** — `import androidx.core.content.ContextCompat`.
- **Liña 15** — `import androidx.work.BackoffPolicy`.
- **Liña 16** — `import androidx.work.Constraints`.
- **Liña 17** — `import androidx.work.ExistingWorkPolicy`.
- **Liña 18** — `import androidx.work.ForegroundInfo`.
- **Liña 19** — `import androidx.work.NetworkType`.
- **Liña 20** — `import androidx.work.OneTimeWorkRequest`.
- **Liña 21** — `import androidx.work.WorkManager`.
- **Liña 22** — `import androidx.work.Worker`.
- **Liña 23** — `import androidx.work.WorkerParameters`.
- **Liña 24** — `import okhttp3.OkHttpClient`.
- **Liña 25** — `import java.util.concurrent.Callable`.
- **Liña 26** — `import java.util.concurrent.Executors`.
- **Liña 27** — `import java.util.concurrent.TimeUnit`.
- **Liña 29–39** — KDoc: upload buat hotu iha `UploadQueue` utilizadór ba Google Drive iha background (WorkManager), serbisu foreground ho notifikasaun "Uploading X of N" — kontinua bainhira app taka; WorkManager hahú fali depois reboot, bainhira rede fila (halai só bainhira online), ka depois falla (backoff). Só transporte muda ba ne'e: kada fotografia sai `/images/<email>/<image_id>.jpg` rasik iha Drive, watcher laptop → Kafka → Spark hanesan uluk.
- **Liña 40** — `class UploadWorker(context: Context, params: WorkerParameters) : Worker(context, params) {`
- **Liña 42** — `companion object {`
- **Liña 43** — `private const val UNIQUE_WORK = "photo-upload"`.
- **Liña 44** — `private const val CHANNEL_ID = "uploads"`.
- **Liña 45** — `private const val PROGRESS_NOTIFICATION_ID = 1001`.
- **Liña 46** — `private const val DONE_NOTIFICATION_ID = 1002`.
- **Liña 47–49** — Komentáriu: fotografia haruka ba Drive iha tempu hanesan; sobrepoin balun subar round-trip iha link neineik; barak liu fahe largura banda hanesan no konvida rate limit Drive.
- **Liña 50** — `private const val PARALLELISM = 3`.
- **Liña 52** — KDoc: hahú (ka kontinua depois) upload buat ne'ebé iha fila.
- **Liña 53** — `fun start(context: Context) = enqueue(context, ExistingWorkPolicy.APPEND_OR_REPLACE)`.
- **Liña 55–56** — KDoc: garante fila tuan (app oho, login expira...) serbisu hela; la halo buat ida se serbisu ona.
- **Liña 57** — `fun resumeIfPending(context: Context) {`
- **Liña 58** — `val email = SessionStore(context).email ?: return`.
- **Liña 59** — `Thread {` — SQLite la iha thread UI.
- **Liña 60** — `if (UploadQueue.get(context).hasQueued(email)) enqueue(context, ExistingWorkPolicy.KEEP)`.
- **Liña 61** — `}.start()`.
- **Liña 62** — `}`.
- **Liña 64** — `private fun enqueue(context: Context, policy: ExistingWorkPolicy) {`
- **Liña 65** — `val request = OneTimeWorkRequest.Builder(UploadWorker::class.java)`.
- **Liña 66** — `.setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())` — só ho rede.
- **Liña 67** — `.setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)` — retry 30 s, 60 s, 120 s...
- **Liña 68** — `.build()`.
- **Liña 69** — `WorkManager.getInstance(context).enqueueUniqueWork(UNIQUE_WORK, policy, request)` — serbisu úniku.
- **Liña 70** — `}`.
- **Liña 71** — `}` — remata companion.

**5. Walkthrough ezekusaun**

Ekran upload → `UploadWorker.start(ctx)` → WorkManager hein rede → `doWork()`. App loke fali depois reboot → `resumeIfPending` → se iha `QUEUED`, `KEEP` (la duplika serbisu).

**6. Tipu dadus**

Konstante `String`/`Int`; `request`: `OneTimeWorkRequest`; `policy`: `ExistingWorkPolicy`.

**7. Ligasaun OOP**

- **Herança**: `UploadWorker : Worker` (AndroidX).
- **Companion object**: métodu "estátiku" `start`, `resumeIfPending`.
- **Builder**: `OneTimeWorkRequest.Builder`, `Constraints.Builder`.

**8. Impaktu integrasaun**

WorkManager garante upload kontinua maske agrikultór taka app — importante iha rede rurál.

**9. Checkpoint ba principiante**

**P:** Tanba sá `enqueueUniqueWork`?
**R:** Atu iha serbisu upload ida de'it iha tempu ida (la iha worker rua upload fotografia hanesan).

#### D.3.2 Cliente HTTP, `doWork`, `signInNeeded`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/UploadWorker.kt 73-145}}

**2. Objetivu**

Serbisu prinsipál: hetan utilizadór no fila, hatudu progresu, hetan token no pasta Drive, upload fotografia iha grupu (paralelu 3, 12 kada volta), marka rezultadu, atualiza istória lokál, trata login/rede, no notifikasaun remata.

**3. Fundamentu Python**

*(Kotlin)* `override fun doWork(): Result`; `?: return ...`; `try { } catch { return ... } ?: run { ... }`; `pool.invokeAll(lista Callable).map { it.get() }`; `for ((item, outcome) in results) when (outcome) { is X -> ... }`; `isStopped` (propriedade herda).

**4. Esplikasaun liña-ba-liña**

- **Liña 73** — `private val apiClient = OkHttpClient()`.
- **Liña 74** — Komentáriu: upload fotografia bele naruk liu chamada API normál iha konesaun rurál neineik.
- **Liña 75** — `private val uploadClient = apiClient.newBuilder()`.
- **Liña 76** — `.writeTimeout(120, TimeUnit.SECONDS)`.
- **Liña 77** — `.readTimeout(60, TimeUnit.SECONDS)`.
- **Liña 78** — `.build()`.
- **Liña 80** — `override fun doWork(): Result {`
- **Liña 81** — `val ctx = applicationContext`.
- **Liña 82** — `val session = SessionStore(ctx)`.
- **Liña 83** — `val email = session.email ?: return Result.success()` — komentáriu: logout: liña hein ba utilizadór ne'e.
- **Liña 84** — `val queue = UploadQueue.get(ctx)`.
- **Liña 85** — `if (!queue.hasQueued(email)) return Result.success()`.
- **Liña 87** — `showProgress(queue.counts(email))`.
- **Liña 88** — `val token = DriveAuth.accessTokenBlocking(ctx) ?: return signInNeeded(queue, email)`.
- **Liña 89** — `val folders = try {`
- **Liña 90** — `session.folders(apiClient, token)` — `DriveRest.myFolders` (Modulu 03).
- **Liña 91** — `} catch (e: Exception) {`
- **Liña 92** — `return Result.retry()` — komentáriu: offline / Drive problema.
- **Liña 93** — `} ?: run {` — `null` = admin seidauk partilla pasta.
- **Liña 94** — `notifyDone(ctx.getString(R.string.upload_done_no_access))`.
- **Liña 95** — `return Result.failure()`.
- **Liña 96** — `}`.
- **Liña 98** — `val history = HistoryStore(ctx, email)`.
- **Liña 99** — `val pool = Executors.newFixedThreadPool(PARALLELISM)`.
- **Liña 100** — `try {`
- **Liña 101** — `while (!isStopped) {`
- **Liña 102** — `val batch = queue.nextQueued(email, PARALLELISM * 4)` — 12.
- **Liña 103** — `if (batch.isEmpty()) break`.
- **Liña 104** — `val results = pool.invokeAll(batch.map { item ->`
- **Liña 105** — `Callable { item to DriveUploader.upload(ctx, uploadClient, apiClient, item, folders) }`.
- **Liña 106** — `}).map { it.get() }` — hein hotu; lista `Pair<Item, Outcome>`.
- **Liña 108** — `var transient = false`.
- **Liña 109** — `var signIn = false`.
- **Liña 110** — `val nowPending = mutableListOf<org.json.JSONObject>()`.
- **Liña 111** — `for ((item, outcome) in results) when (outcome) {`
- **Liña 112** — `is DriveUploader.Outcome.Uploaded -> {`
- **Liña 113** — `queue.markUploaded(item, outcome.driveFileId)`.
- **Liña 114** — `nowPending.add(history.pendingItem(item.imageId, outcome.driveFileId, item.location))` — istória "pending".
- **Liña 115** — `}`.
- **Liña 116** — `is DriveUploader.Outcome.Rejected -> queue.markRejected(item, outcome.error)`.
- **Liña 117** — `is DriveUploader.Outcome.Failed -> queue.markFailed(item, outcome.error)`.
- **Liña 118** — `is DriveUploader.Outcome.Transient -> transient = true` — hela `QUEUED`.
- **Liña 119** — `DriveUploader.Outcome.SignInNeeded -> signIn = true` — `object` (la presiza `is`).
- **Liña 120** — `}`.
- **Liña 121** — `history.merge(nowPending)` — komentáriu: rai ida kada volta, la'ós kada fotografia.
- **Liña 122** — `showProgress(queue.counts(email))`.
- **Liña 123** — `if (signIn) return signInNeeded(queue, email)`.
- **Liña 124** — `if (transient) return Result.retry()` — komentáriu: WorkManager koko fali depois; liña hela fila.
- **Liña 125** — `}`.
- **Liña 126** — `} finally {`
- **Liña 127** — `pool.shutdownNow()`.
- **Liña 128** — `}`.
- **Liña 129** — `if (isStopped) return Result.retry()`.
- **Liña 131** — `val counts = queue.counts(email)`.
- **Liña 132** — `notifyDone(`
- **Liña 133** — `if (counts.failed + counts.rejected == 0)`.
- **Liña 134** — `ctx.getString(R.string.upload_done_all, counts.uploaded)`.
- **Liña 135** — `else`.
- **Liña 136** — `ctx.getString(R.string.upload_done_some_failed, counts.uploaded, counts.failed + counts.rejected)`.
- **Liña 137** — `)`.
- **Liña 138** — `return Result.success()`.
- **Liña 139** — `}`.
- **Liña 141** — KDoc: husik fotografia iha fila: loke app depois login kontinua.
- **Liña 142** — `private fun signInNeeded(queue: UploadQueue, email: String): Result {`
- **Liña 143** — `notifyDone(applicationContext.getString(R.string.upload_paused_sign_in, queue.counts(email).queued))`.
- **Liña 144** — `return Result.failure()`.
- **Liña 145** — `}`.

**5. Walkthrough ezekusaun**

Fotografia 30: volta 1 (12, paralelu 3) → 12 Uploaded; volta 2 → 11 Uploaded + 1 Transient → `Result.retry()` → WorkManager hein 30 s → `doWork` fali → kontinua 7 sira seluk.

**6. Tipu dadus**

`email`: `String`; `token`: `String`; `folders`: `DriveRest.Folders`; `pool`: `ExecutorService`; `results`: `List<Pair<Item, Outcome>>`; return `Result`.

**7. Ligasaun OOP**

- **Polimorfizmu**: `when (outcome) { is Uploaded -> ... }` — hili hahalok tuir sub-klase `sealed class Outcome`.
- **Override** `doWork` (métodu abstratu `Worker`).
- **Komposisaun**: worker na'in `apiClient`, `uploadClient`.

**8. Impaktu integrasaun**

`DriveUploader.upload` (Modulu 03 D.9.3) → Drive → watcher (Modulu 02). Istória "pending" atualiza ho `resultsSince` depois (Modulu 03 D.9.9).

**9. Checkpoint ba principiante**

**P:** Diferensa `Result.retry()` no `Result.failure()`?
**R:** `retry` — WorkManager koko fali depois (backoff); `failure` — para (presiza intervensaun: login ka asesu pasta).

#### D.3.3 Notifikasaun

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/UploadWorker.kt 147-207}}

**2. Objetivu**

Harii kanál notifikasaun (Android 8+), intent atu loke app, notifikasaun progresu foreground ("Uploading X of N"), no notifikasaun remata (Android 13+ presiza permisaun).

**3. Fundamentu Python**

*(Kotlin)* `Build.VERSION.SDK_INT >= ...`; Builder `NotificationCompat.Builder(...)...build()`; `setForegroundAsync(info).get()`; `or` (bitwise ba flag).

**4. Esplikasaun liña-ba-liña**

- **Liña 147** — Komentáriu seksaun notifikasaun.
- **Liña 149** — `private fun ensureChannel() {`
- **Liña 150** — `if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {`
- **Liña 151** — `val manager = applicationContext.getSystemService(NotificationManager::class.java)`.
- **Liña 152–154** — `manager.createNotificationChannel(NotificationChannel(CHANNEL_ID, ..., NotificationManager.IMPORTANCE_LOW))`.
- **Liña 155** — `}`.
- **Liña 156** — `}`.
- **Liña 158** — `private fun openAppIntent(): PendingIntent = PendingIntent.getActivity(`
- **Liña 159** — `applicationContext, 0,`
- **Liña 160** — `Intent(applicationContext, MainActivity::class.java).addFlags(Intent.FLAG_ACTIVITY_SINGLE_TOP),`
- **Liña 161** — `PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE,`
- **Liña 162** — `)`.
- **Liña 164–166** — KDoc: halai serbisu foreground ho notifikasaun "Uploading X of N"; se Android la permite agora, upload kontinua hanesan serbisu background normál.
- **Liña 167** — `private fun showProgress(counts: UploadQueue.Counts) {`
- **Liña 168** — `ensureChannel()`.
- **Liña 169** — `val notification = NotificationCompat.Builder(applicationContext, CHANNEL_ID)`.
- **Liña 170** — `.setSmallIcon(android.R.drawable.stat_sys_upload)`.
- **Liña 171** — `.setContentTitle(...upload_notification_title)`.
- **Liña 172** — `.setContentText(...upload_progress, counts.done, counts.total)`.
- **Liña 173** — `.setProgress(counts.total, counts.done, false)`.
- **Liña 174** — `.setOngoing(true)`.
- **Liña 175** — `.setOnlyAlertOnce(true)`.
- **Liña 176** — `.setContentIntent(openAppIntent())`.
- **Liña 177** — `.build()`.
- **Liña 178** — `val info = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q)`.
- **Liña 179** — `ForegroundInfo(PROGRESS_NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)`.
- **Liña 180** — `else`.
- **Liña 181** — `ForegroundInfo(PROGRESS_NOTIFICATION_ID, notification)`.
- **Liña 182** — `try {`
- **Liña 183** — `setForegroundAsync(info).get()`.
- **Liña 184** — `} catch (e: Exception) {`
- **Liña 185** — Komentáriu: la permite husi background agora — kontinua la ho nia.
- **Liña 186** — `}`.
- **Liña 187** — `}`.
- **Liña 189** — `private fun notifyDone(text: String) {`
- **Liña 190** — `val ctx = applicationContext`.
- **Liña 191** — `if (Build.VERSION.SDK_INT >= 33 &&`
- **Liña 192** — `ContextCompat.checkSelfPermission(ctx, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED`.
- **Liña 193** — `) return`.
- **Liña 194** — `ensureChannel()`.
- **Liña 195** — `NotificationManagerCompat.from(ctx).notify(`
- **Liña 196** — `DONE_NOTIFICATION_ID,`
- **Liña 197** — `NotificationCompat.Builder(ctx, CHANNEL_ID)`.
- **Liña 198** — `.setSmallIcon(android.R.drawable.stat_sys_upload_done)`.
- **Liña 199** — `.setContentTitle(...)`.
- **Liña 200** — `.setContentText(text)`.
- **Liña 201** — `.setStyle(NotificationCompat.BigTextStyle().bigText(text))`.
- **Liña 202** — `.setAutoCancel(true)`.
- **Liña 203** — `.setContentIntent(openAppIntent())`.
- **Liña 204** — `.build()`.
- **Liña 205** — `)`.
- **Liña 206** — `}`.
- **Liña 207** — `}` — remata klase.

**5. Walkthrough ezekusaun**

Upload 30 → notifikasaun "Uploading 12 of 30" (barra) → remata "30 photos uploaded" (*testu ilustrativu*; testu loloos iha `strings.xml`).

**6. Tipu dadus**

`notification`: `Notification`; `info`: `ForegroundInfo`; métodu → `Unit` ka `PendingIntent`.

**7. Ligasaun OOP**

Padraun **Builder** (NotificationCompat); métodu `private` (enkapsulamentu).

**8. Impaktu integrasaun**

Foreground service evita Android oho upload naruk.

**9. Checkpoint ba principiante**

**P:** Tanba sá upload uza notifikasaun foreground?
**R:** Android bele hapara serbisu background naruk; foreground (ho notifikasaun) hatudu ba utilizadór no Android katak serbisu importante.

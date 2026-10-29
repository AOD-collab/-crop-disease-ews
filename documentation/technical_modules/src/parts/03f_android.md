
### D.9 Aplikasaun Android (Kotlin)

*Nota:* bloku sira ne'e Kotlin. Pasu 3 "Fundamentu Python" iha ne'e esplika **fundamentu Kotlin** (haree tabela A.3) no kompara ho Python.

#### D.9.1 `DriveAuth.kt` — token OAuth iha telemóvel

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/DriveAuth.kt 1-46}}

**2. Objetivu**

Fó *access token* OAuth Drive ba agrikultór ne'ebé login ona, ho Google Identity Services `AuthorizationClient`. Telemóvel nunka rai password ka chave; Google fó token badak ba konta agrikultór **rasik**.

**3. Fundamentu Python**

*(Kotlin)* `object` = singleton (hanesan modulu Python). `fun f(): T = espresaun` = funsaun ho korpu espresaun ida. `String?` = bele `null`. `try { ... } catch (e: Exception) { null }` iha Kotlin mak **espresaun** — fó valór. `Tasks.await(...)` hein tarefa asinkrónu (bloke thread).

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `package com.cropdisease.app` — pakote aplikasaun.
- **Liña 3** — `import android.content.Context` — "kontekstu" Android (asesu ba rekursu app).
- **Liña 4** — `AuthorizationRequest` — pedidu scope OAuth.
- **Liña 5** — `Identity` — pontu tama Google Identity Services.
- **Liña 6** — `Scope` — scope OAuth.
- **Liña 7** — `Tasks` — utilidade ba `Task` Google (`await`).
- **Liña 9–23** — KDoc (komentáriu dokumentasaun): asesu OAuth Drive ba agrikultór ne'ebé login, ho `AuthorizationClient`; telemóvel nunka rai password ka chave; Google fó access token badak ba konta agrikultór RASIK; pasta konta sentrál bele asesu tanba admin partilla (`drive_share_user.py`); scope Drive tomak tanba app tenke hakerek no lee pasta na'in konta sentrál, ne'ebé scope `drive.file` la haree; di'ak ba projetu akadémiku bainhira app iha Testing ho test user.
- **Liña 24** — `object DriveAuth {` — singleton.
- **Liña 25** — `const val DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"` — hanesan `SCOPES` laptop.
- **Liña 27** — `fun authorizationRequest(): AuthorizationRequest =`
- **Liña 28** — `AuthorizationRequest.builder()` — padraun Builder.
- **Liña 29** — `.setRequestedScopes(listOf(Scope(DRIVE_SCOPE)))`.
- **Liña 30** — `.build()`.
- **Liña 32–37** — KDoc: access token foun ba konta autoriza ona, ka `null` se utilizadór tenke login/konsente fali (depois ba `LoginActivity`); bloke: bolu husi thread background de'it; Google rai no refresh token, entaun bolu molok kada batch barata.
- **Liña 38** — `fun accessTokenBlocking(context: Context): String? = try {`
- **Liña 39** — `val result = Tasks.await(`
- **Liña 40** — `Identity.getAuthorizationClient(context).authorize(authorizationRequest())` — husu autorizasaun (Task).
- **Liña 41** — `)` — hein rezultadu.
- **Liña 42** — `if (result.hasResolution()) null else result.accessToken` — presiza intervensaun utilizadór (login/konsentimentu) → `null`; se lae, token.
- **Liña 43** — `} catch (e: Exception) {`
- **Liña 44** — `null`.
- **Liña 45** — `}` — remata try/catch.
- **Liña 46** — `}` — remata `object`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `UploadWorker` (thread background) bolu `DriveAuth.accessTokenBlocking(ctx)` → Google fó token (cache) → `<access token>` (token la hatudu). Se utilizadór hasai permisaun → `hasResolution() == true` → `null` → `DriveUploader` fó `Outcome.SignInNeeded`.

**6. Tipu dadus**

| Naran | Tipu Kotlin |
|---|---|
| `DRIVE_SCOPE` | `String` (`const`) |
| `authorizationRequest()` | `AuthorizationRequest` |
| `accessTokenBlocking(context: Context)` | `String?` |
| `result` | `AuthorizationResult` (inferidu) |

**7. Ligasaun OOP**

`object DriveAuth` mak **singleton Kotlin** (objetu ida, harii bainhira uza dahuluk). `AuthorizationRequest.builder()...build()` mak padraun **Builder** husi biblioteka Google.

**8. Impaktu integrasaun**

`DriveUploader.upload` bolu kada koko (liña 69). Scope hanesan laptop. Token iha ne'e mak token **agrikultór**, la'ós token konta sentrál (laptop).

**9. Checkpoint ba principiante**

**P:** Tanba sá funsaun ne'e tenke bolu husi thread background?
**R:** `Tasks.await` bloke to'o Google responde. Iha thread UI, ida ne'e sei halo aplikasaun "hakmatek" (ANR).

#### D.9.2 `DriveUploader.kt` — header, konstante no `sealed class Outcome`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/DriveUploader.kt 1-43}}

**2. Objetivu**

Haruka fotografia IDA husi fila ba pasta agrikultór `/images/<email>/<image_id>.jpg` ho `appProperties` ne'ebé watcher laptop lee. Bytes haruka hanesan iha telemóvel (la hadi'ak, la re-encode). Define limite tamañu, koko fali, no rezultadu posível (`Outcome`).

**3. Fundamentu Python**

*(Kotlin)* `private const val` — konstante ne'ebé só objetu ne'e haree. `sealed class` — klase inan ho sub-klase **fixu** (kompiladór hatene hotu; `when` bele kompleta). `data class X(val a: String) : Outcome()` — sub-klase dadus. `object SignInNeeded : Outcome()` — sub-klase singleton (la iha dadus). `15 * 1024 * 1024` = 15 MB.

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `package com.cropdisease.app`.
- **Liña 3** — `import android.Manifest` — naran permisaun.
- **Liña 4** — `import android.content.Context`.
- **Liña 5** — `import android.content.pm.PackageManager` — kontrola permisaun.
- **Liña 6** — `import android.net.Uri` — enderesu fotografia.
- **Liña 7** — `import android.os.Build` — versaun Android.
- **Liña 8** — `import android.provider.MediaStore` — galeria.
- **Liña 9** — `import androidx.core.content.ContextCompat`.
- **Liña 10** — `import okhttp3.OkHttpClient` — cliente HTTP.
- **Liña 11** — `import java.io.IOException`.
- **Liña 12** — `import java.util.Locale` — formatu númeru.
- **Liña 14–24** — KDoc: haruka fotografia IDA ba pasta `/images/<email>/<image_id>.jpg`, ho appProperties ne'ebé watcher lee (`image_id`, `original_filename`, `location`, `gps_*`); uza husi `UploadWorker`; bytes haruka hanesan iha telemóvel — nunka hadi'ak ka re-encode — atu Spark no CNN analiza orijinál; bloke: bolu husi thread background.
- **Liña 25** — `object DriveUploader {`
- **Liña 26** — Komentáriu: limite hanesan watcher laptop (`MAX_IMAGE_SIZE`).
- **Liña 27** — `const val MAX_PHOTO_BYTES = 15 * 1024 * 1024` — 15 MB.
- **Liña 28–29** — Komentáriu: koko kada fotografia ba konesaun monu / rate limit / 5xx, hein 1 s, 2 s, 4 s (+ jitter).
- **Liña 30** — `private const val ATTEMPTS = 4`.
- **Liña 31** — `private const val RETRY_BASE_DELAY_MS = 1000L` — `L` = `Long`.
- **Liña 33** — `sealed class Outcome {`
- **Liña 34** — `data class Uploaded(val driveFileId: String) : Outcome()` — susesu, ho ID arkivu.
- **Liña 35** — KDoc: la'ós fotografia útil (tipu sala, la bele lee, boot liu): la koko fali.
- **Liña 36** — `data class Rejected(val error: String) : Outcome()`.
- **Liña 37** — KDoc: Drive rejeita (ezemplu la iha asesu pasta): "Retry failed" bele ajuda depois.
- **Liña 38** — `data class Failed(val error: String) : Outcome()`.
- **Liña 39** — KDoc: problema rede/Drive ne'ebé liu koko fali: hela iha fila.
- **Liña 40** — `data class Transient(val error: String) : Outcome()`.
- **Liña 41** — KDoc: presiza login Google fali: hela iha fila.
- **Liña 42** — `object SignInNeeded : Outcome()`.
- **Liña 43** — `}` — remata `Outcome`.

**5. Walkthrough ezekusaun**

`UploadWorker` simu `Outcome` no hili: `Uploaded` → marka "uploaded"; `Rejected` → marka erru (la koko fali); `Transient`/`SignInNeeded` → hela iha fila (Modulu 06).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `MAX_PHOTO_BYTES` | `Int` (15,728,640) |
| `ATTEMPTS` | `Int` |
| `RETRY_BASE_DELAY_MS` | `Long` |
| `Outcome` | `sealed class` ho sub-klase 5 |

**7. Ligasaun OOP**

- **Herança implementadu**: `Uploaded`, `Rejected`, `Failed`, `Transient` (data class) no `SignInNeeded` (object) **herda** `Outcome`.
- **Polimorfizmu + abstrasaun**: funsaun `upload` fó tipu `Outcome` (abstratu); chamadór uza `when (outcome) { is Uploaded -> ...; is Rejected -> ... }`.
- **Klase taka (sealed)**: kompiladór garante katak kazu hotu trata.
- **Singleton**: `object DriveUploader`, `object SignInNeeded`.

**8. Impaktu integrasaun**

Limite 15 MB proteje Drive/Spark husi arkivu boot liu. `UploadWorker.kt` (Modulu 06) uza `Outcome`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `SignInNeeded` mak `object` no la'ós `data class`?
**R:** La iha dadus atu lori (la iha erru/ID) — objetu ida de'it sufisiente.

#### D.9.3 `DriveUploader.upload` — valida, properties, retry

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/DriveUploader.kt 45-95}}

**2. Objetivu**

Valida fotografia (tipu JPEG/PNG, la mamuk, ≤ 15 MB), harii `appProperties`, no koko upload to'o dala 4: token foun kada koko; koko segundu buka uluk se koko anterior to'o Drive ona; trata kódigu HTTP; hein ho backoff.

**3. Fundamentu Python**

*(Kotlin)* `?: return X` — se `null`, sai husi funsaun. `x?.let { ... }` — halai bloku se la `null` (`it` = valór). `mutableMapOf("a" to b)` — dict. `for (attempt in 1..ATTEMPTS)` — 1..4. `when { kond -> ação }`. `Unit` — "la halo buat ida" (hanesan `pass`). `1L shl (attempt - 1)` — 1, 2, 4. `(0..500).random()` — aleatóriu 0–500.

**4. Esplikasaun liña-ba-liña**

- **Liña 45** — `fun upload(`
- **Liña 46** — `context: Context, uploadClient: OkHttpClient, apiClient: OkHttpClient,` — cliente HTTP rua (upload ho timeout naruk, API normál).
- **Liña 47** — `item: UploadQueue.Item, folders: DriveRest.Folders,` — fotografia iha fila; pasta agrikultór.
- **Liña 48** — `): Outcome {`
- **Liña 49** — `val invalid = context.getString(R.string.error_invalid_file)` — mensajen lian lokál.
- **Liña 50** — `if (item.mimeType != "image/jpeg" && item.mimeType != "image/png") return Outcome.Rejected(invalid)`.
- **Liña 51** — `val bytes = readPhotoBytes(context, Uri.parse(item.uri)) ?: return Outcome.Rejected(invalid)`.
- **Liña 52** — `if (bytes.isEmpty() || bytes.size > MAX_PHOTO_BYTES) return Outcome.Rejected(invalid)`.
- **Liña 54** — `val properties = mutableMapOf(`
- **Liña 55** — `"image_id" to item.imageId,` — UUID ne'ebé app harii.
- **Liña 56** — `"original_filename" to item.displayName.take(80),` — maximu karakter 80.
- **Liña 57** — `)`.
- **Liña 58** — `item.location?.let { properties["location"] = it.take(100) }`.
- **Liña 59** — `if (item.gpsLat != null && item.gpsLon != null) {`
- **Liña 60** — `properties["gps_lat"] = String.format(Locale.US, "%.6f", item.gpsLat)` — desimál 6, pontu (la'ós vírgula).
- **Liña 61** — `properties["gps_lon"] = String.format(Locale.US, "%.6f", item.gpsLon)`.
- **Liña 62** — `item.gpsAccM?.let { properties["gps_acc_m"] = String.format(Locale.US, "%.0f", it) }` — presizaun metru.
- **Liña 63** — `}`.
- **Liña 65** — `var lastError: Exception? = null`.
- **Liña 66** — `for (attempt in 1..ATTEMPTS) {`
- **Liña 67–68** — Komentáriu: token foun kada koko: Google rai nia, no upload naruk liu ~1 oras token válidu.
- **Liña 69** — `val token = DriveAuth.accessTokenBlocking(context) ?: return Outcome.SignInNeeded`.
- **Liña 70** — `try {`
- **Liña 71–72** — Komentáriu: koko fali kontrola uluk se koko anterior to'o Drive duni (só resposta lakon).
- **Liña 73** — `val driveFileId = (if (attempt > 1) DriveRest.findImage(apiClient, token, folders.imagesId, item.imageId) else null)`.
- **Liña 74** — `?: DriveRest.uploadImage(uploadClient, token, folders.imagesId, bytes, item.mimeType, item.imageId, properties)` — se la hetan, upload.
- **Liña 75** — `return Outcome.Uploaded(driveFileId)`.
- **Liña 76** — `} catch (e: DriveRest.DriveHttpException) {`
- **Liña 77** — `when {`
- **Liña 78** — `e.code == 401 && attempt < ATTEMPTS -> Unit` — token refresh iha koko tuirmai.
- **Liña 79** — `e.code == 401 -> return Outcome.SignInNeeded`.
- **Liña 80** — `DriveRest.isRetryable(e) -> Unit` — 429/5xx/403 rate limit: koko fali.
- **Liña 81** — `e.code == 403 || e.code == 404 -> return Outcome.Failed(context.getString(R.string.error_drive_no_access))` — la iha asesu pasta.
- **Liña 82** — `else -> return Outcome.Failed(e.message ?: ...)`.
- **Liña 83** — `}`.
- **Liña 84** — `lastError = e`.
- **Liña 85** — `} catch (e: IOException) {`
- **Liña 86** — `lastError = e` — komentáriu: konesaun monu / timeout: koko fali.
- **Liña 87** — `} catch (e: Exception) {`
- **Liña 88** — `return Outcome.Failed(e.message ?: ...)`.
- **Liña 89** — `}`.
- **Liña 90** — `if (attempt < ATTEMPTS) {`
- **Liña 91** — `Thread.sleep(RETRY_BASE_DELAY_MS * (1L shl (attempt - 1)) + (0..500).random())` — 1 s, 2 s, 4 s + 0–0.5 s.
- **Liña 92** — `}`.
- **Liña 93** — `}` — remata `for`.
- **Liña 94** — `return Outcome.Transient(lastError?.message ?: ...)` — koko hotu falla: hela iha fila.
- **Liña 95** — `}` — remata funsaun.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* koko 1: `uploadImage` → `IOException` (sinál monu) → hein ~1.3 s. Koko 2: `findImage` → `"1AbCdEf"` (upload anterior to'o Drive!) → `Outcome.Uploaded("1AbCdEf")` — la iha fotografia daruak. Koko seluk: 403 "insufficientPermissions" → `Outcome.Failed("no access")`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `item` | `UploadQueue.Item` (`imageId`, `uri`, `mimeType`, `displayName`, `location?`, `gpsLat?`, `gpsLon?`, `gpsAccM?`) |
| `folders` | `DriveRest.Folders` |
| `bytes` | `ByteArray` |
| `properties` | `MutableMap<String, String>` |
| `token` | `String` |
| `lastError` | `Exception?` |
| return | `Outcome` |

**7. Ligasaun OOP**

Fó sub-klase `Outcome` diferente tuir situasaun — **polimorfizmu** via `sealed class`. Kaer `DriveRest.DriveHttpException` (sub-klase `IOException`) **molok** `IOException` — orden importante tanba herança.

**8. Impaktu integrasaun**

Chave `properties` (`image_id`, `location`, `gps_lat`, `gps_lon`) lee husi `build_drive_event`/`_device_gps` (D.5) no `_clean_drive_event` (D.4.2). Telemóvel la haruka `captured_at_client` iha funsaun ne'e (watcher lee nia se iha; Inferidu: la haruka husi kódigu ne'e).

**9. Checkpoint ba principiante**

**P:** Tanba sá koko segundu bolu `findImage` uluk?
**R:** Koko dahuluk bele to'o Drive maibé resposta lakon. Buka uluk evita fotografia daruak (no Spark analiza dala rua).

#### D.9.4 `DriveUploader.readPhotoBytes`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/DriveUploader.kt 97-121}}

**2. Objetivu**

Lee bytes fotografia **ho** tag EXIF GPS bainhira Android permite (`setRequireOriginal` presiza permisaun `ACCESS_MEDIA_LOCATION` no URI MediaStore). Se lae, lee URI normál (Android bele hasai GPS).

**3. Fundamentu Python**

*(Kotlin)* `?.use { return it.readBytes() }` — loke stream, lee, taka; `return` iha lambda inline sai husi funsaun. `if (A && B && C)`. `Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q` — Android 10+.

**4. Esplikasaun liña-ba-liña**

- **Liña 97–100** — KDoc: bytes fotografia HO tag EXIF GPS bainhira Android permite; `setRequireOriginal` presiza `ACCESS_MEDIA_LOCATION` no URI MediaStore (dokumentu galeria konverte ho `getMediaUri`); seluk uza URI normál, ne'ebé Android bele hasai tag GPS.
- **Liña 101** — `fun readPhotoBytes(context: Context, uri: Uri): ByteArray? {`
- **Liña 102** — `val mediaLocation = ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_MEDIA_LOCATION) ==`
- **Liña 103** — `PackageManager.PERMISSION_GRANTED` — permisaun fó ona?
- **Liña 104** — `if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && mediaLocation && uri.scheme == "content") {`
- **Liña 105** — `try {`
- **Liña 106** — `val media = if (uri.authority == MediaStore.AUTHORITY) uri else MediaStore.getMediaUri(context, uri)`.
- **Liña 107** — `if (media != null) {`
- **Liña 108** — `context.contentResolver.openInputStream(MediaStore.setRequireOriginal(media))` — husu orijinál (ho GPS).
- **Liña 109** — `?.use { return it.readBytes() }`.
- **Liña 110** — `}`.
- **Liña 111** — `} catch (e: Exception) {`
- **Liña 112** — Komentáriu: la'ós fotografia MediaStore (ezemplu Drive, Downloads) → lee normál iha okos.
- **Liña 113** — `}`.
- **Liña 114** — `}`.
- **Liña 115** — `return try {`
- **Liña 116** — `context.contentResolver.openInputStream(uri)?.use { it.readBytes() }`.
- **Liña 117** — `} catch (e: Exception) {`
- **Liña 118** — `null`.
- **Liña 119** — `}`.
- **Liña 120** — `}` — remata funsaun.
- **Liña 121** — `}` — remata `object DriveUploader`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* Android 13, permisaun fó, fotografia galeria `content://media/external/images/media/123` → lee orijinál ho GPS EXIF → Spark `extract_metadata` hetan GPS EXIF. La ho permisaun → lee normál → GPS EXIF bele lakon → Spark uza GPS telemóvel (`gps_lat`/`gps_lon` iha appProperties).

**6. Tipu dadus**

`uri`: `Uri`; `mediaLocation`: `Boolean`; `media`: `Uri?`; return `ByteArray?`.

**7. Ligasaun OOP**

`context.contentResolver` — objetu Android ne'ebé fó stream; `use` taka objetu `InputStream` (interface `Closeable`).

**8. Impaktu integrasaun**

Bytes ne'e mak bytes ne'ebé Spark download no `extract_metadata` lee (Modulu 01 D.3.5).

**9. Checkpoint ba principiante**

**P:** Tanba sá GPS EXIF bele lakon?
**R:** Android hasai lokalizasaun husi fotografia ba privasidade, exetu app iha permisaun `ACCESS_MEDIA_LOCATION` no husu orijinál.

#### D.9.5 `DriveRest.kt` — header, tipu dadus no helper HTTP

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/DriveRest.kt 1-70}}

**2. Objetivu**

Objetu ida ho buat hotu ne'ebé app halo ho Drive (REST v3) ho token agrikultór. Define URL, klase erru HTTP, klase dadus (`Me`, `Folders`), helper `authed`/`execute`/`getJson`/`q`, no `aboutMe`.

**3. Fundamentu Python**

*(Kotlin)* `class DriveHttpException(val code: Int, message: String) : IOException(message)` — sub-klase ho propriedade `code` no bolu konstrutór inan. `private fun` — só iha objetu. `"Bearer $token"` — string template. `response.body?.string()?.take(300)`. `.use { }` — taka resposta. `it.body!!` — obriga la null.

**4. Esplikasaun liña-ba-liña**

- **Liña 1** — `package com.cropdisease.app`.
- **Liña 3** — `toHttpUrl` — string → URL.
- **Liña 4** — `toMediaType`.
- **Liña 5** — `toMediaTypeOrNull`.
- **Liña 6** — `OkHttpClient`.
- **Liña 7** — `Request`.
- **Liña 8** — `toRequestBody`.
- **Liña 9** — `JSONArray`.
- **Liña 10** — `JSONObject`.
- **Liña 11** — `IOException`.
- **Liña 13–31** — KDoc: buat hotu app halo ho Drive (REST v3) ho token agrikultór (`DriveAuth`); troka chamada ba API projetu antigu. Lista funsaun: `myFolders()` (pasta PRIVADU rua: `/images/<email>/` writer, `/results/<email>/` reader), `uploadImage()` (Android → Drive), `findResults()` (rezultadu Spark), `resetMarker()` (factory reset ikus), `myHistory()` (upload + rezultadu iha forma JSON hanesan `GET /my/history` antigu). Funsaun hotu BLOKE — bolu husi thread background.
- **Liña 32** — `object DriveRest {`
- **Liña 33** — `private const val DRIVE = "https://www.googleapis.com/drive/v3"`.
- **Liña 34** — `private const val UPLOAD = "https://www.googleapis.com/upload/drive/v3"`.
- **Liña 35** — `private val JSON = "application/json; charset=utf-8".toMediaType()`.
- **Liña 37** — KDoc: erru HTTP Drive; 401 = login fali, 403/404 = la iha asesu pasta.
- **Liña 38** — `class DriveHttpException(val code: Int, message: String) : IOException(message)`.
- **Liña 40** — `data class Me(val email: String, val displayName: String?)`.
- **Liña 42** — KDoc: pasta privadu agrikultór.
- **Liña 43** — `data class Folders(val imagesId: String, val resultsId: String)`.
- **Liña 45** — `private const val FOLDER_MIME = "application/vnd.google-apps.folder"`.
- **Liña 46** — `private const val RESET_MARKER_NAME = "_reset_marker.json"` — hanesan `storage/reset.py`.
- **Liña 48** — `private fun authed(token: String) = Request.Builder().header("Authorization", "Bearer $token")`.
- **Liña 50** — `private fun execute(client: OkHttpClient, request: Request): okhttp3.Response {`
- **Liña 51** — `val response = client.newCall(request).execute()` — haruka (bloke).
- **Liña 52** — `if (!response.isSuccessful) {` — la'ós 2xx:
- **Liña 53** — `val body = response.body?.string()?.take(300)`.
- **Liña 54** — `response.close()`.
- **Liña 55** — `throw DriveHttpException(response.code, "Google Drive error ${response.code}: $body")`.
- **Liña 56** — `}`.
- **Liña 57** — `return response`.
- **Liña 58** — `}`.
- **Liña 60** — `private fun getJson(client: OkHttpClient, token: String, url: String): JSONObject =`
- **Liña 61** — `execute(client, authed(token).url(url).get().build()).use { JSONObject(it.body!!.string()) }`.
- **Liña 63** — `private fun q(value: String) = value.replace("\\", "\\\\").replace("'", "\\'")` — escape hanesan `_escape_query_value`.
- **Liña 65** — KDoc: konta Google login (identidade utilizadór iha app).
- **Liña 66** — `fun aboutMe(client: OkHttpClient, token: String): Me {`
- **Liña 67** — `val user = getJson(client, token, "$DRIVE/about?fields=user(emailAddress,displayName)")`.
- **Liña 68** — `.getJSONObject("user")`.
- **Liña 69** — `return Me(user.getString("emailAddress").lowercase(), user.optString("displayName").ifEmpty { null })`.
- **Liña 70** — `}`.

**5. Walkthrough ezekusaun**

`aboutMe(client, token)` → `GET .../about?fields=user(emailAddress,displayName)` → `Me("agrikultor@example.com", "Agrikultor")` (*ezemplu ilustrativu*). Resposta 401 → `DriveHttpException(401, "Google Drive error 401: ...")`.

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `DriveHttpException.code` | `Int` |
| `Me` | `email: String`, `displayName: String?` |
| `Folders` | `imagesId: String`, `resultsId: String` |
| `execute` | `okhttp3.Response` |
| `getJson` | `JSONObject` |

**7. Ligasaun OOP**

- **Herança**: `DriveHttpException : IOException(message)` — bolu konstrutór inan (hanesan `super().__init__` Python) no aumenta propriedade `code` (hanesan `DriveError.status_code` iha laptop).
- **Data class**: `Me`, `Folders` (klase dadus imutável).
- **Enkapsulamentu**: `private` (`DRIVE`, `authed`, `execute`, `getJson`, `q`) — Kotlin **blokeia** asesu husi liur (diferente husi konvensaun `_` Python).
- **Singleton**: `object DriveRest`.

**8. Impaktu integrasaun**

`RESET_MARKER_NAME` no escape query hanesan ho laptop — kontratu Python ↔ Kotlin.

**9. Checkpoint ba principiante**

**P:** Diferensa `private` Kotlin no `_nome` Python?
**R:** `private` Kotlin mak regra kompiladór (labele asesu husi liur). `_nome` Python mak só konvensaun (bele asesu).

#### D.9.6 `myFolders` no `uploadImage` (upload resumable)

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/DriveRest.kt 72-139}}

**2. Objetivu**

`myFolders`: buka pasta rua agrikultór entre pasta ne'ebé partilla ho nia (`sharedWithMe`), hatene ida images no ida results (tuir `appProperties.kind` ka `canAddChildren`). `uploadImage`: upload **resumable** (pasu 2: harii sesaun → haruka bytes), funsiona ba fotografia > 5 MB.

**3. Fundamentu Python**

*(Kotlin)* `var images: String? = null`; `for (i in 0 until files.length())`; `when { A || B -> ... }`; `JSONObject().put(...).put(...)` chaining; `properties.filterValues { it.isNotEmpty() }`.

**4. Esplikasaun liña-ba-liña**

- **Liña 72–77** — KDoc: buka pasta privadu rua entre pasta partilla; naran rua = email; ida ne'ebé bele aumenta arkivu mak `/images/<email>/`, read-only mak `/results/<email>/`; `null` se admin seidauk partilla.
- **Liña 78** — `fun myFolders(client: OkHttpClient, token: String, email: String): Folders? {`
- **Liña 79** — `val url = "$DRIVE/files".toHttpUrl().newBuilder()`.
- **Liña 80** — `.addQueryParameter(`
- **Liña 81** — `"q",`
- **Liña 82** — `"name = '${q(email)}' and mimeType = '$FOLDER_MIME' and trashed = false and sharedWithMe"`.
- **Liña 83** — `)`.
- **Liña 84** — `.addQueryParameter("fields", "files(id,capabilities(canAddChildren),appProperties)")`.
- **Liña 85** — `.build().toString()`.
- **Liña 86** — `val files = getJson(client, token, url).optJSONArray("files") ?: return null`.
- **Liña 87** — `var images: String? = null`.
- **Liña 88** — `var results: String? = null`.
- **Liña 89** — `for (i in 0 until files.length()) {`
- **Liña 90** — `val f = files.getJSONObject(i)`.
- **Liña 91** — `val kind = f.optJSONObject("appProperties")?.optString("kind").orEmpty()` — `"user_images"`/`"user_results"` (husi `ensure_folder` laptop).
- **Liña 92** — `val canUpload = f.optJSONObject("capabilities")?.optBoolean("canAddChildren") == true`.
- **Liña 93** — `when {`
- **Liña 94** — `kind == "user_images" || (kind.isEmpty() && canUpload) -> images = f.getString("id")`.
- **Liña 95** — `kind == "user_results" || (kind.isEmpty() && !canUpload) -> results = f.getString("id")`.
- **Liña 96** — `}`.
- **Liña 97** — `}`.
- **Liña 98** — `return if (images != null && results != null) Folders(images, results) else null`.
- **Liña 99** — `}`.
- **Liña 101–106** — KDoc: upload fotografia ida ba `/images/<email>/` hanesan `<imageId>.jpg/.png` (resumable: funsiona ba > 5 MB); `properties` sai appProperties Drive ne'ebé watcher lee; fó ID arkivu.
- **Liña 107** — `fun uploadImage(`
- **Liña 108** — `client: OkHttpClient,`
- **Liña 109** — `token: String,`
- **Liña 110** — `imagesFolderId: String,`
- **Liña 111** — `bytes: ByteArray,`
- **Liña 112** — `mimeType: String,`
- **Liña 113** — `imageId: String,`
- **Liña 114** — `properties: Map<String, String>,`
- **Liña 115** — `): String {`
- **Liña 116** — `val extension = if (mimeType == "image/png") ".png" else ".jpg"`.
- **Liña 117** — `val metadata = JSONObject()`.
- **Liña 118** — `.put("name", "$imageId$extension")` — naran arkivu = `image_id`.
- **Liña 119** — `.put("mimeType", mimeType)`.
- **Liña 120** — `.put("parents", JSONArray().put(imagesFolderId))`.
- **Liña 121** — `.put("appProperties", JSONObject(properties.filterValues { it.isNotEmpty() }))`.
- **Liña 123** — Komentáriu: 1. hahú sesaun upload resumable.
- **Liña 124** — `val sessionUrl = execute(`
- **Liña 125** — `client,`
- **Liña 126** — `authed(token)`.
- **Liña 127** — `.url("$UPLOAD/files?uploadType=resumable&fields=id")`.
- **Liña 128** — `.header("X-Upload-Content-Type", mimeType)`.
- **Liña 129** — `.header("X-Upload-Content-Length", bytes.size.toString())`.
- **Liña 130** — `.post(metadata.toString().toRequestBody(JSON))` — POST metadata.
- **Liña 131** — `.build()`.
- **Liña 132** — `).use { it.header("Location") } ?: throw IOException("Google Drive did not return an upload URL")` — URL sesaun.
- **Liña 134** — Komentáriu: 2. haruka bytes.
- **Liña 135** — `return execute(`
- **Liña 136** — `client,`
- **Liña 137** — `authed(token).url(sessionUrl).put(bytes.toRequestBody(mimeType.toMediaTypeOrNull())).build()` — PUT bytes.
- **Liña 138** — `).use { JSONObject(it.body!!.string()).getString("id") }`.
- **Liña 139** — `}`.

**5. Walkthrough ezekusaun**

Haree C.3 pasu 3. `myFolders(... "agrikultor@example.com")` → pasta rua partilla → `Folders(imagesId="1Img...", resultsId="1Res...")`. Admin seidauk halai `drive_share_user.py` → `null` → app hatudu mensajen "la iha asesu".

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `myFolders` return | `Folders?` |
| `kind` | `String` |
| `canUpload` | `Boolean` |
| `metadata` | `JSONObject` |
| `sessionUrl` | `String` |
| `uploadImage` return | `String` (ID arkivu) |

**7. Ligasaun OOP**

Harii objetu `Folders(images, results)` (data class). `Request.Builder` chaining — padraun Builder (OkHttp).

**8. Impaktu integrasaun**

Naran arkivu `<image_id>.jpg` no `appProperties` liga ho watcher (D.5.2) no Spark (`_clean_drive_event`, D.4.2). `kind` husi `drive_client._find_or_create_user_folder` (D.2.9).

**9. Checkpoint ba principiante**

**P:** Tanba sá upload "resumable" iha pasu rua?
**R:** Pasu 1 haruka metadata no hetan URL sesaun; pasu 2 haruka bytes. Metodu ne'e suporta arkivu boot (> 5 MB) no rede la estável.

#### D.9.7 `findImage`, `isRetryable`, `findResults`, `resetMarker`, download

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/DriveRest.kt 141-220}}

**2. Objetivu**

Buka fotografia iha pasta images (antes koko fali upload); hatene erru ne'ebé vale koko fali; buka rezultadu ba `image_id` barak (pedidu ida kada 20); lee reset marker foun liu; download JSON, bytes no thumbnail.

**3. Fundamentu Python**

*(Kotlin)* `imageIds.chunked(20)` — fahe lista ba grupu 20 (hanesan `range(0, n, 20)` Python). `joinToString(" or ") { ... }`. `removeSuffix(".json")`. `Regex("=s\\d+$")`. `.ifEmpty { return null }`.

**4. Esplikasaun liña-ba-liña**

- **Liña 141–143** — KDoc: ID arkivu `<imageId>.jpg/.png` iha pasta images, ka `null`; uza molok koko fali upload ne'ebé resposta lakon, atu retry nunka tau fotografia hanesan dala rua.
- **Liña 144** — `fun findImage(client, token, imagesFolderId, imageId): String? {`
- **Liña 145** — `val url = "$DRIVE/files".toHttpUrl().newBuilder()`.
- **Liña 146** — `.addQueryParameter(`
- **Liña 147** — `"q",`
- **Liña 148** — `"'$imagesFolderId' in parents and trashed = false and " +`
- **Liña 149** — `"(name = '${q(imageId)}.jpg' or name = '${q(imageId)}.png')"`.
- **Liña 150** — `)`.
- **Liña 151** — `.addQueryParameter("fields", "files(id)")`.
- **Liña 152** — `.addQueryParameter("pageSize", "1")`.
- **Liña 153** — `.build().toString()`.
- **Liña 154** — `val files = getJson(client, token, url).optJSONArray("files") ?: return null`.
- **Liña 155** — `return if (files.length() > 0) files.getJSONObject(0).getString("id") else null`.
- **Liña 156** — `}`.
- **Liña 158–159** — KDoc: loos ba erru Drive ne'ebé vale koko fali: rate limit (429, ka 403 ho `rateLimitExceeded`) no 5xx.
- **Liña 160** — `fun isRetryable(e: DriveHttpException): Boolean =`
- **Liña 161** — `e.code == 429 || e.code >= 500 ||`
- **Liña 162** — `(e.code == 403 && e.message.orEmpty().contains("rateLimitExceeded", ignoreCase = true))` — regra hanesan `_should_back_off` laptop.
- **Liña 164–168** — KDoc: `imageIds` ne'ebé iha ona arkivu rezultadu; pedidu Drive IDA kada ID 20; fó `image_id → ID arkivu <image_id>.json`.
- **Liña 169** — `fun findResults(`
- **Liña 170** — `client, token, resultsFolderId, imageIds: Collection<String>,`
- **Liña 171** — `): Map<String, String> {`
- **Liña 172** — `val found = mutableMapOf<String, String>()`.
- **Liña 173** — `for (chunk in imageIds.chunked(20)) {`
- **Liña 174** — `val names = chunk.joinToString(" or ") { "name = '${q(it)}.json'" }`.
- **Liña 175** — `val url = "$DRIVE/files".toHttpUrl().newBuilder()`.
- **Liña 176** — `.addQueryParameter("q", "'$resultsFolderId' in parents and trashed = false and ($names)")`.
- **Liña 177** — `.addQueryParameter("fields", "files(id,name)")`.
- **Liña 178** — `.addQueryParameter("pageSize", "100")`.
- **Liña 179** — `.build().toString()`.
- **Liña 180** — `val files = getJson(client, token, url).optJSONArray("files") ?: JSONArray()`.
- **Liña 181** — `for (i in 0 until files.length()) {`
- **Liña 182** — `val f = files.getJSONObject(i)`.
- **Liña 183** — `found[f.getString("name").removeSuffix(".json")] = f.getString("id")`.
- **Liña 184** — `}`.
- **Liña 185** — `}`.
- **Liña 186** — `return found`.
- **Liña 187** — `}`.
- **Liña 189–191** — KDoc: `reset_at` husi reset marker foun liu ne'ebé factory reset laptop husik iha `/results/<email>/` (`storage/reset.py`), ka `null`.
- **Liña 192** — `fun resetMarker(client, token, resultsFolderId): String? {`
- **Liña 193** — `val url = "$DRIVE/files".toHttpUrl().newBuilder()`.
- **Liña 194** — `.addQueryParameter("q", "'$resultsFolderId' in parents and trashed = false and name = '$RESET_MARKER_NAME'")`.
- **Liña 195** — `.addQueryParameter("orderBy", "modifiedTime desc")`.
- **Liña 196** — `.addQueryParameter("pageSize", "1")`.
- **Liña 197** — `.addQueryParameter("fields", "files(appProperties)")`.
- **Liña 198** — `.build().toString()`.
- **Liña 199** — `val files = getJson(...).optJSONArray("files") ?: return null`.
- **Liña 200** — `if (files.length() == 0) return null`.
- **Liña 201** — `return files.getJSONObject(0).optJSONObject("appProperties")?.optString("reset_at")?.ifEmpty { null }` — lee appProperties (la download JSON).
- **Liña 202** — `}`.
- **Liña 204** — `fun downloadJson(client, token, fileId): JSONObject =`
- **Liña 205** — `execute(client, authed(token).url("$DRIVE/files/$fileId?alt=media").get().build())`.
- **Liña 206** — `.use { JSONObject(it.body!!.string()) }`.
- **Liña 208** — `fun downloadBytes(client, token, fileId): ByteArray =`
- **Liña 209** — `execute(client, authed(token).url("$DRIVE/files/$fileId?alt=media").get().build())`.
- **Liña 210** — `.use { it.body!!.bytes() }` — ezemplu `_processed.jpg`.
- **Liña 212–213** — KDoc: preview ki'ik Google (KB balun duke fotografia tomak), ka `null`.
- **Liña 214** — `fun downloadThumbnail(client, token, fileId, sizePx: Int): ByteArray? {`
- **Liña 215** — `val link = getJson(client, token, "$DRIVE/files/$fileId?fields=thumbnailLink")`.
- **Liña 216** — `.optString("thumbnailLink").ifEmpty { return null }`.
- **Liña 217** — Komentáriu: `thumbnailLink` remata ho `"=s220"`; husu tamañu ne'ebé ekran presiza.
- **Liña 218** — `val sized = link.replace(Regex("=s\\d+$"), "=s$sizePx")`.
- **Liña 219** — `return execute(client, authed(token).url(sized).get().build()).use { it.body?.bytes() }`.
- **Liña 220** — `}`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* fotografia 45 hein rezultadu → `findResults` halo pedidu 3 → `{"a1b2...": "1XyZ...", ...}` (sira ne'ebé Spark hakerek ona). `resetMarker` → `"2026-10-28T03:00:00.000000Z"` → app kompara ho valór rai → se foun, hamoos istória.

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `findImage` | `String?` |
| `isRetryable` | `Boolean` |
| `findResults` | `Map<String, String>` |
| `resetMarker` | `String?` |
| `downloadJson` | `JSONObject` |
| `downloadBytes` | `ByteArray` |
| `downloadThumbnail` | `ByteArray?` |

**7. Ligasaun OOP**

Métodu iha singleton `object DriveRest`; kada funsaun la iha estadu (stateless).

**8. Impaktu integrasaun**

`findResults` lee arkivu `<image_id>.json` ne'ebé `publish_result_to_drive` hakerek (D.7.1). `resetMarker` lee `reset_at` husi `write_reset_markers` (D.8.1). `isRetryable` hanesan `_should_back_off` laptop (D.2.4).

**9. Checkpoint ba principiante**

**P:** Tanba sá `findResults` fahe ID ba grupu 20?
**R:** Query ho `or` barak naruk liu bele falla; grupu 20 = pedidu uitoan no query badak (hanesan `list_files_in_folders` laptop).

#### D.9.8 `listFiles` no `myHistory`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/DriveRest.kt 222-280}}

**2. Objetivu**

`listFiles`: lista arkivu (paginasaun) to'o `max`. `myHistory`: upload agrikultór (pasta images) foun uluk, kombina ho rezultadu (pasta results, liuhusi appProperties `image_id`), forma JSON hanesan `GET /my/history` antigu; rezultadu ne'ebé fotografia la iha ona mós hatudu.

**3. Fundamentu Python**

*(Kotlin)* `do { ... } while (kond)`; `results.associateBy { chave }` — lista → map tuir chave (hanesan `{k(x): x for x in lista}`); `substringBeforeLast('.')`; `mutableSetOf<String>()`; `for ((k, v) in map)` — destructuring; `sortedByDescending { }`.

**4. Esplikasaun liña-ba-liña**

- **Liña 222** — `private fun listFiles(client, token, query: String, max: Int): List<JSONObject> {`
- **Liña 223** — `val out = mutableListOf<JSONObject>()`.
- **Liña 224** — `var pageToken: String? = null`.
- **Liña 225** — `do {`
- **Liña 226** — `val builder = "$DRIVE/files".toHttpUrl().newBuilder()`.
- **Liña 227** — `.addQueryParameter("q", query)`.
- **Liña 228** — `.addQueryParameter("orderBy", "createdTime desc")` — foun uluk.
- **Liña 229** — `.addQueryParameter("pageSize", "100")`.
- **Liña 230** — `.addQueryParameter("fields", "nextPageToken,files(id,name,createdTime,appProperties)")`.
- **Liña 231** — `pageToken?.let { builder.addQueryParameter("pageToken", it) }`.
- **Liña 232** — `val body = getJson(client, token, builder.build().toString())`.
- **Liña 233** — `val files = body.optJSONArray("files") ?: JSONArray()`.
- **Liña 234** — `for (i in 0 until files.length()) out.add(files.getJSONObject(i))`.
- **Liña 235** — `pageToken = body.optString("nextPageToken").ifEmpty { null }`.
- **Liña 236** — `} while (pageToken != null && out.size < max)`.
- **Liña 237** — `return out.take(max)`.
- **Liña 238** — `}`.
- **Liña 240–249** — KDoc: upload utilizadór (pasta images) foun uluk, kada ida kombina ho rezultadu husi `/results/<email>/` (se Spark hakerek ona), forma hanesan `GET /my/history` antigu: `{image_id, drive_file_id, processed_file_id, location, uploaded_at, outcome, prediction: {predicted_class, prediction_confidence, risk_level} | null, error_message}`; `outcome` tuir regra: failed → pending (seidauk iha rezultadu) → healthy → diseased.
- **Liña 250** — `fun myHistory(client, token, folders: Folders, max: Int = 200): List<JSONObject> {`
- **Liña 251** — `val images = listFiles(`
- **Liña 252** — `client, token,`
- **Liña 253** — `"'${folders.imagesId}' in parents and trashed = false and mimeType != '$FOLDER_MIME'", max`.
- **Liña 254** — `)`.
- **Liña 255** — `val results = listFiles(`
- **Liña 256** — `client, token,`
- **Liña 257** — `"'${folders.resultsId}' in parents and trashed = false and mimeType = 'application/json'", max` — JSON de'it (inklui reset marker; nia la iha `image_id`).
- **Liña 258** — `)`.
- **Liña 259** — `val resultByImage = results.associateBy {`
- **Liña 260** — `it.optJSONObject("appProperties")?.optString("image_id").orEmpty()`.
- **Liña 261** — `}`.
- **Liña 263** — `val items = mutableListOf<JSONObject>()`.
- **Liña 264** — `val seen = mutableSetOf<String>()`.
- **Liña 265** — `for (image in images) {`
- **Liña 266** — `val props = image.optJSONObject("appProperties") ?: JSONObject()`.
- **Liña 267** — `val imageId = props.optString("image_id").ifEmpty { image.optString("name").substringBeforeLast('.') }` — `image_id` husi appProperties ka naran arkivu.
- **Liña 268** — `seen.add(imageId)`.
- **Liña 269** — `items.add(historyItem(imageId, image.getString("id"), props.optString("location"),`
- **Liña 270** — `image.optString("createdTime"), resultByImage[imageId]?.optJSONObject("appProperties")))`.
- **Liña 271** — `}`.
- **Liña 272** — Komentáriu: rezultadu ne'ebé fotografia la lista ona (ezemplu hamoos husi Drive) hatudu nafatin.
- **Liña 273** — `for ((imageId, result) in resultByImage) {`
- **Liña 274** — `if (imageId.isEmpty() || imageId in seen) continue`.
- **Liña 275** — `val props = result.optJSONObject("appProperties") ?: JSONObject()`.
- **Liña 276** — `items.add(historyItem(imageId, props.optString("drive_image_file_id"), props.optString("location"),`
- **Liña 277** — `props.optString("uploaded_at"), props))`.
- **Liña 278** — `}`.
- **Liña 279** — `return items.sortedByDescending { it.optString("uploaded_at") }`.
- **Liña 280** — `}`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* pasta images: `a1.jpg`, `b2.jpg`; pasta results: `a1.json` (COMPLETED, blight). → item 2: `a1` (`outcome = "diseased"`), `b2` (`outcome = "pending"`).

**6. Tipu dadus**

| Naran | Tipu |
|---|---|
| `listFiles` return | `List<JSONObject>` |
| `resultByImage` | `Map<String, JSONObject>` |
| `seen` | `MutableSet<String>` |
| `myHistory` return | `List<JSONObject>` |

**7. Ligasaun OOP**

Uza `Folders` (data class) hanesan parámetru — objetu lori ID rua hamutuk.

**8. Impaktu integrasaun**

Lee appProperties `summary` husi `publish_result_to_drive` (D.7.1): `image_id`, `status`, `predicted_class`, `prediction_confidence`, `risk_level`, `location`, `uploaded_at`, `drive_image_file_id`, `processed_file_id`, `error_message`.

**9. Checkpoint ba principiante**

**P:** Tanba sá `myHistory` mós hatudu rezultadu ne'ebé fotografia la iha ona?
**R:** Agrikultór bele hamoos fotografia iha Drive maibé rezultadu (pasta results, read-only) hela; app hatudu nafatin istória.

#### D.9.9 `resultsSince` no `historyItem`

**1. Kódigu orijinál**

{{code android_app/app/src/main/java/com/cropdisease/app/DriveRest.kt 282-346}}

**2. Objetivu**

`resultsSince`: rezultadu hotu ne'ebé Spark hakerek/atualiza depois `sinceIso` (pájina 1000), plus `modifiedTime` foun liu — telemóvel mantein rezultadu fotografia rihun ho pedidu uitoan. `historyItem`: harii item istória ho regra `outcome`.

**3. Fundamentu Python**

*(Kotlin)* `Pair<A, B>` + `items to newest` (harii pár, hanesan tuple Python). `?: continue`. Komparasaun string `modified > newest` (ISO kompara hanesan tempu). `JSONObject.NULL` — `null` JSON.

**4. Esplikasaun liña-ba-liña**

- **Liña 282–288** — KDoc: rezultadu hotu ne'ebé Spark hakerek ka atualiza iha `/results/<email>/` depois `sinceIso` (null = hotu), hanesan item istória, plus `modifiedTime` foun liu (haruka fali dala tuirmai); telemóvel mantein rezultadu fotografia rihun ho pedidu uitoan (1000 kada pájina).
- **Liña 289** — `fun resultsSince(`
- **Liña 290** — `client, token, folders: Folders, sinceIso: String?,`
- **Liña 291** — `): Pair<List<JSONObject>, String?> {`
- **Liña 292** — `var query = "'${folders.resultsId}' in parents and trashed = false and mimeType = 'application/json'"`.
- **Liña 293** — `if (sinceIso != null) query += " and modifiedTime > '${q(sinceIso)}'"`.
- **Liña 294** — `val items = mutableListOf<JSONObject>()`.
- **Liña 295** — `var newest = sinceIso`.
- **Liña 296** — `var pageToken: String? = null`.
- **Liña 297** — `do {`
- **Liña 298** — `val builder = "$DRIVE/files".toHttpUrl().newBuilder()`.
- **Liña 299** — `.addQueryParameter("q", query)`.
- **Liña 300** — `.addQueryParameter("pageSize", "1000")`.
- **Liña 301** — `.addQueryParameter("fields", "nextPageToken,files(id,modifiedTime,appProperties)")`.
- **Liña 302** — `pageToken?.let { builder.addQueryParameter("pageToken", it) }`.
- **Liña 303** — `val body = getJson(client, token, builder.build().toString())`.
- **Liña 304** — `val files = body.optJSONArray("files") ?: JSONArray()`.
- **Liña 305** — `for (i in 0 until files.length()) {`
- **Liña 306** — `val f = files.getJSONObject(i)`.
- **Liña 307** — `val props = f.optJSONObject("appProperties") ?: continue`.
- **Liña 308** — `val imageId = props.optString("image_id")`.
- **Liña 309** — `if (imageId.isEmpty()) continue` — salta reset marker.
- **Liña 310** — `items.add(historyItem(imageId, props.optString("drive_image_file_id"), props.optString("location"),`
- **Liña 311** — `props.optString("uploaded_at"), props))`.
- **Liña 312** — `val modified = f.optString("modifiedTime")`.
- **Liña 313** — `if (newest == null || modified > newest) newest = modified`.
- **Liña 314** — `}`.
- **Liña 315** — `pageToken = body.optString("nextPageToken").ifEmpty { null }`.
- **Liña 316** — `} while (pageToken != null)`.
- **Liña 317** — `return items to newest`.
- **Liña 318** — `}`.
- **Liña 320** — `private fun historyItem(`
- **Liña 321** — `imageId: String, driveFileId: String, imageLocation: String, createdTime: String, result: JSONObject?,`
- **Liña 322** — `): JSONObject {`
- **Liña 323** — `val status = result?.optString("status").orEmpty()`.
- **Liña 324** — `val predictedClass = result?.optString("predicted_class").orEmpty()`.
- **Liña 325** — `val outcome = when {`
- **Liña 326** — `status == "FAILED" -> "failed"`.
- **Liña 327** — `status != "COMPLETED" -> "pending"`.
- **Liña 328** — `predictedClass.equals("healthy", ignoreCase = true) -> "healthy"`.
- **Liña 329** — `else -> "diseased"`.
- **Liña 330** — `}`.
- **Liña 331** — `val location = result?.optString("location")?.ifEmpty { null } ?: imageLocation.ifEmpty { null }`.
- **Liña 332** — `return JSONObject()`.
- **Liña 333** — `.put("image_id", imageId)`.
- **Liña 334** — `.put("drive_file_id", driveFileId)`.
- **Liña 335** — `.put("processed_file_id", result?.optString("processed_file_id").orEmpty())`.
- **Liña 336** — `.put("location", location ?: JSONObject.NULL)`.
- **Liña 337** — `.put("uploaded_at", result?.optString("uploaded_at")?.ifEmpty { null } ?: createdTime)`.
- **Liña 338** — `.put("outcome", outcome)`.
- **Liña 339** — `.put("prediction", if (status == "COMPLETED") JSONObject()`
- **Liña 340** — `.put("predicted_class", predictedClass.ifEmpty { "unknown" })`.
- **Liña 341** — `.put("prediction_confidence", result?.optString("prediction_confidence")?.toDoubleOrNull() ?: 0.0)` — appProperties string → `Double`.
- **Liña 342** — `.put("risk_level", result?.optString("risk_level").orEmpty().ifEmpty { "UNKNOWN" })`.
- **Liña 343** — `else JSONObject.NULL)`.
- **Liña 344** — `.put("error_message", result?.optString("error_message")?.ifEmpty { null } ?: JSONObject.NULL)`.
- **Liña 345** — `}` — remata `historyItem`.
- **Liña 346** — `}` — remata `object DriveRest`.

**5. Walkthrough ezekusaun**

*Ezemplu ilustrativu:* `resultsSince(..., "2026-10-20T01:00:00.000Z")` → rezultadu 3 foun → `newest = "2026-10-20T01:15:04.120Z"` → telemóvel rai no haruka dala tuirmai. `historyItem("a1", "1AbC", "Aileu", "...", {"status": "COMPLETED", "predicted_class": "blight", "prediction_confidence": "0.97", "risk_level": "HIGH"})` → `{"outcome": "diseased", "prediction": {"predicted_class": "blight", "prediction_confidence": 0.97, "risk_level": "HIGH"}, ...}`.

**6. Tipu dadus**

| Funsaun | Return |
|---|---|
| `resultsSince` | `Pair<List<JSONObject>, String?>` |
| `historyItem` | `JSONObject` |
| `outcome` | `String` (`failed`/`pending`/`healthy`/`diseased`) |

**7. Ligasaun OOP**

`Pair` mak klase jenérika Kotlin. `historyItem` `private` — enkapsulamentu formál.

**8. Impaktu integrasaun**

Regra `outcome` hanesan ho API antigu; dadus mai husi `summary` iha `publish_result_to_drive`. Ekran Home/History app (Modulu 06) uza item sira ne'e.

**9. Checkpoint ba principiante**

**P:** Saida mak `outcome` ba fotografia ne'ebé seidauk iha arkivu rezultadu?
**R:** `result` = `null` → `status = ""` → la'ós `"FAILED"` no la'ós `"COMPLETED"` → `"pending"`.

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.cropdisease.app"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.cropdisease.app"
        minSdk = 24
        targetSdk = 34
        versionCode = 1
        versionName = "1.0"
    }

    buildFeatures {
        viewBinding = true
        buildConfig = true
    }

    buildTypes {
        release {
            isMinifyEnabled = false
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions {
        jvmTarget = "17"
    }
}

dependencies {
    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.appcompat:appcompat:1.7.0")
    implementation("com.google.android.material:material:1.12.0")
    implementation("androidx.constraintlayout:constraintlayout:2.1.4")
    implementation("androidx.cardview:cardview:1.0.0")
    implementation("androidx.activity:activity-ktx:1.9.0")
    // Background photo upload that survives the app closing / reboots (UploadWorker)
    implementation("androidx.work:work-runtime:2.9.1")

    // Networking (Google Drive REST API v3)
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    // Google sign-in + Drive authorization (OAuth) on the phone
    implementation("com.google.android.gms:play-services-auth:21.2.0")
    implementation("org.json:json:20240303")
}

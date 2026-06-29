# Data Dictionary

---

## Music Metadata

### Schema

- track_id
- track_title
- artist
- genre
- language_code
- tempo_bpm
- energy_rms
- spectral_flatness
- danceability_proxy
- instrumentalness
- music_valence
- lyrics_theme
- lyrics_sentiment
- granular_mood

### Audio Features

- **tempo_bpm**:
  - The tempo or speed of the song measured in Beats Per Minute. Physically, high BPM tracks (>120) stimulate the sympathetic nervous system while low BPM tracks (<75) help induce physiological down-regulation.
  - Example value: `142.0`
- **energy_rms**:
  - The overall physical energy of the track calculated from the Root-Mean-Square of the audio amplitude. It indicates acoustic intensity (e.g., heavy electronic bass or distorted guitars yield high values).
  - Example value: `0.1850`
- **spectral_flatness**:
  - Measures the flatness of the audio spectrum. A value close to 1.0 indicates a noise-like sound (e.g., heavy distortion or synth noise), while a value close to 0.0 indicates a clean, harmonic melody.
  - Example value: `0.0045`
- **danceability_proxy**:
  - A proxy for rhythm stability computed via the hyperbolic tangent of onset strength variance. It evaluates how regular and predictable the beats are. Regular, rhythmic patterns help the body adapt and stabilize Heart Rate Variability (HRV).
  - Example value: `0.7200`
- **instrumentalness**:
  - The estimated ratio of instrumental sounds versus vocal presence. Values approaching 1.0 represent purely instrumental music. Instrumental tracks generally support hyper-focus and cognitive stress reduction better than lyric-heavy tracks.
  - Example value: `0.8500`

### Multimodal Sentiment

- **music_valence**:
  - The musical positiveness conveyed by the melody and harmony alone, ranging from 0.0 (sad, tense, or minor keys) to 1.0 (happy, bright, or major keys). Separating this from lyrics sentiment allows the system to detect complex audio states (e.g., upbeat music with depressing lyrics).
  - Example value: `0.2800`
- **lyrics_theme**:
  - The overarching semantic theme of the song's context mapped via text dict matching (`Comfort`, `Motivation`, `Loneliness`, `Heartbreak`, `Escapism`, `General`). It gives the dbt pipeline a psychological layer to identify user coping mechanisms.
  - Example value: `"Escapism"`
- **lyrics_sentiment**:
  - The emotional polarity score mapped from the text theme, ranging from -1.0 (highly negative/dark/sad) to 1.0 (highly positive/cheerful/bright).
  - Example value: `-0.1500`
- **granular_mood**:
  - The highly contextual, fine-grained emotional classification of the track. Computed from a tri-axial multimodal matrix evaluating physical energy, musical valence, and lyrics sentiment to isolate emotional paradoxes:
    - `Euphoric` (High Energy + Bright Music + Positive Lyrics)
    - `Tense/Hype` (High Energy + Aggressive Music / Neutral Lyrics)
    - `Bitter-Sweet / Happy-Sad` (High Energy + Negative Lyrics Paradox)
    - `Peaceful` (Low Energy + Bright Music / Positive Lyrics)
    - `Gloomy` (Low Energy + Dark Music / Negative Lyrics)
  - Example value: `"Bitter-Sweet / Happy-Sad"`

---

## Human Profile Metadata

### Schema

- user_id
- username
- dob
- gender
- cultural_region
- baseline_resting_hr
- baseline_hrv_sdnn
- created_at
- updated_at

### Demographics and Context

- **dob**:
  - The date of birth of the user. Storing the date instead of a static age integer ensures data freshness, enabling downstream dbt transformations to dynamically calculate age without structural degradation over time.
  - Example value: `2004-05-15`
- **gender**:
  - The biological gender code used to establish baseline physiological threshold ranges. Restricted to standardized single-character tokens: `M` (Male), `F` (Female), `O` (Other).
  - Example value: `"M"`
- **cultural_region**:
  - The country or cultural origin of the user standardized under the **ISO 3166-1 alpha-2** standard. This allows the system to analyze cross-cultural audio perception and musical theme affinity.
  - Example value: `"VI"`

### Baseline Biometrics

- **baseline_resting_hr**:
  - The user's average baseline Resting Heart Rate (RHR) measured in Beats Per Minute (BPM) during calm states. Serves as the physiological anchor point to monitor post-audio heart rate acceleration or down-regulation.
  - Example value: `68.5`
- **baseline_hrv_sdnn**:
  - The user's baseline Heart Rate Variability (HRV) calculated via the SDNN metric (Standard Deviation of NN intervals) in milliseconds (ms). It measures autonomic nervous system balance; lower baseline values indicate higher pre-existing stress levels.
  - Example value: `45.2`

### Audit Metadata

- **created_at**:
  - The precise system timestamp recording when the user profile was originally registered in the database.
  - Example value: `2026-06-29 08:30:00.000`
- **updated_at**:
  - The system timestamp tracking the most recent modification to the user profile settings or baseline biometric recalibration.
  - Example value: `2026-06-29 16:34:21.123`

---

## Biometric Events

### Schema

- event_id
- user_id
- timestamp
- heart_rate
- hrv_rmssd
- eda_microsiemens
- motion_status

### Physiological Metrics

- **event_id**:
  - A unique identifier generated as a UUIDv4 for each real-time biometric event log.
  - Example value: `"a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d"`
- **user_id**:
  - A foreign key linking directly to the Human Profile table, identifying which user generated the biometric signal.
  - Example value: `"e9b1a5c2-7d3f-4a8b-9c0d-1e2f3a4b5c6d"`
- **timestamp**:
  - The precise high-resolution Unix epoch timestamp measured in milliseconds tracking when the physiological event occurred.
  - Example value: `1782725400000`
- **heart_rate**:
  - The instantaneous heart rate of the user measured in Beats Per Minute (BPM). Higher values indicate sympathetic nervous activation (arousal/exercise), while lower values indicate down-regulation.
  - Example value: `72.4`
- **hrv_rmssd**:
  - The short-term Heart Rate Variability (HRV) calculated via the RMSSD method (Root Mean Square of Successive Differences) in milliseconds (ms). This metric captures immediate, rapid changes in vagal tone and parasympathetic activity induced by external stimuli (e.g., music).
  - Example value: `38.5`
- **eda_microsiemens**:
  - The Electrodermal Activity (EDA), also known as Galvanic Skin Response (GSR), measured in microsiemens (μS). It measures skin conductance driven by sweat gland activation, serving as a highly sensitive proxy for cognitive stress and emotional arousal.
  - Example value: `2.45`

### Contextual Status

- **motion_status**:
  - The physical movement state of the user captured by accelerometer sensors. This acts as a vital data filtering layer to differentiate between physiological changes caused by physical exercise versus purely emotional/cognitive responses to audio tracks. Standardized tokens include:
    - `RESTING` (Sitting or lying down quietly)
    - `WALKING` (Low-intensity movement)
    - `RUNNING` (High-intensity physical exertion)
    - `STATIONARY` (Active but standing still)
  - Example value: `"RESTING"`
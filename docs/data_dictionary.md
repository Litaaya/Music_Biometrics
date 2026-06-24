# Data Dictionary

---

## Music metadata:

### Schema

- track_id
- track_title
- artist
- genre
- language_code

### Audio Features

- tempo_bpm:
  - The tempo or speed of the song measured in Beats Per Minute. Physically, high BPM tracks (>120) stimulate the sympathetic nervous system while low BPM tracks (<75) help induce physiological down-regulation.
  - Example value: 142.0
- energy_rms:
  - The overall physical energy of the track calculated from the Root-Mean-Square of the audio amplitude. It indicates acoustic intensity (e.g., heavy electronic bass or distorted guitars yield high values).
  - Example value: 0.185
- spectral_flatness:
  - Measures the flatness of the audio spectrum. A value close to 1.0 indicates a noise-like sound (e.g., heavy distortion or synth noise), while a value close to 0.0 indicates a clean, harmonic melody.
  - Example value: 0.0045
- danceability_proxy:
  - A proxy for rhythm stability. It evaluates how regular and predictable the beats are. Regular, rhythmic patterns help the body adapt and stabilize Heart Rate Variability (HRV).
  - Example value: 0.72
- instrumentalness:
  - The estimated ratio of instrumental sounds versus vocal presence. Values approaching 1.0 represent purely instrumental music. Instrumental tracks generally support hyper-focus and cognitive stress reduction better than lyric-heavy tracks.
  - Example value: 0.85

### Multimodel Sentiment

- music_valence:
  - The musical positiveness conveyed by the melody and harmony alone, ranging from 0.0 (sad, tense, or minor keys) to 1.0 (happy, bright, or major keys). Separating this from lyrics sentiment allows the system to detect complex "Happy-Sad" songs (upbeat music with depressing lyrics).
  - Example value:  0.28
- lyrics_theme:
  - The overarching semantic theme of the song's context (e.g., Heartbreak, Motivation, Loneliness, Escapism, Focus). It gives the dbt pipeline a psychological layer to identify user coping mechanisms.
  - Example value: Escapism
- lyrics_sentiment:
  - The emotional polarity of the lyrics text, ranging from -1.0 (highly negative/dark/sad) to 1.0 (highly positive/cheerful/bright).
  - Example value: -0.3500
- granular_mood:
  - The highly contextual, fine-grained emotional classification of the track. Computed from combinations of physical energy and semantic valence:
    - Euphoric (High Energy + High Valence)
    - Tense (High Energy + Low Valence)
    - Peaceful (Low Energy + High Valence)
    - Gloomy (Low Energy + Low Valence)
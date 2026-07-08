{{ config(materialized='view') }}

with source_data as (
    select * from {{ source('lakehouse_raw', 'music_tracks') }}
)

select
    cast(track_id as string) as track_id,
    cast(track_title as string) as track_title,
    cast(artist as string) as artist_name,
    cast(genre as string) as genre,
    cast(language_code as string) as language_code,

    cast(tempo_bpm as double) as tempo_bpm,
    cast(energy_rms as double) as energy_rms,
    cast(spectral_flatness as double) as spectral_flatness,
    cast(danceability_proxy as double) as danceability_proxy,
    cast(instrumentalness as double) as instrumentalness,

    cast(music_valence as double) as music_valence,
    cast(lyrics_theme as string) as lyrics_theme,
    cast(lyrics_sentiment as double) as lyrics_sentiment,
    cast(granular_mood as string) as granular_mood
from source_data
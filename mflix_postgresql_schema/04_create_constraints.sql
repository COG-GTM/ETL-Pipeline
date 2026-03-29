

ALTER TABLE users ADD CONSTRAINT users_password_min_length 
    CHECK (LENGTH(password) >= 8);

ALTER TABLE sessions ADD CONSTRAINT sessions_expires_reasonable 
    CHECK (expires_at <= created_at + INTERVAL '30 days');

ALTER TABLE movies ADD CONSTRAINT movies_poster_url_format 
    CHECK (poster IS NULL OR poster ~* '^https?://.*\.(jpg|jpeg|png|gif|webp)(\?.*)?$');

ALTER TABLE movies ADD CONSTRAINT movies_rated_valid_values 
    CHECK (rated IS NULL OR rated IN ('G', 'PG', 'PG-13', 'R', 'NC-17', 'NR', 'UNRATED', 'APPROVED', 'NOT RATED', 'PASSED', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA'));

ALTER TABLE movies ADD CONSTRAINT movies_type_valid_values 
    CHECK (type IS NULL OR type IN ('movie', 'series', 'episode'));

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_poster_url_format 
    CHECK (poster IS NULL OR poster ~* '^https?://.*\.(jpg|jpeg|png|gif|webp)(\?.*)?$');

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_rated_valid_values 
    CHECK (rated IS NULL OR rated IN ('G', 'PG', 'PG-13', 'R', 'NC-17', 'NR', 'UNRATED', 'APPROVED', 'NOT RATED', 'PASSED', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA'));

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_type_valid_values 
    CHECK (type IS NULL OR type IN ('movie', 'series', 'episode'));


ALTER TABLE movies ADD CONSTRAINT movies_awards_structure 
    CHECK (
        awards IS NULL OR 
        (jsonb_typeof(awards) = 'object' AND 
         (awards ? 'wins' OR awards ? 'nominations' OR awards ? 'text'))
    );

ALTER TABLE movies ADD CONSTRAINT movies_imdb_structure 
    CHECK (
        imdb IS NULL OR 
        (jsonb_typeof(imdb) = 'object' AND 
         (awards ? 'rating' OR awards ? 'votes' OR awards ? 'id'))
    );

ALTER TABLE movies ADD CONSTRAINT movies_tomatoes_structure 
    CHECK (
        tomatoes IS NULL OR 
        (jsonb_typeof(tomatoes) = 'object' AND 
         (tomatoes ? 'viewer' OR tomatoes ? 'critic' OR tomatoes ? 'fresh' OR tomatoes ? 'rotten'))
    );

ALTER TABLE movies ADD CONSTRAINT movies_plot_embedding_structure 
    CHECK (
        plot_embedding IS NULL OR 
        jsonb_typeof(plot_embedding) = 'array'
    );

ALTER TABLE movies ADD CONSTRAINT movies_plot_embedding_voyage_structure 
    CHECK (
        plot_embedding_voyage_3_large IS NULL OR 
        jsonb_typeof(plot_embedding_voyage_3_large) = 'array'
    );

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_awards_structure 
    CHECK (
        awards IS NULL OR 
        (jsonb_typeof(awards) = 'object' AND 
         (awards ? 'wins' OR awards ? 'nominations' OR awards ? 'text'))
    );

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_imdb_structure 
    CHECK (
        imdb IS NULL OR 
        (jsonb_typeof(imdb) = 'object' AND 
         (imdb ? 'rating' OR imdb ? 'votes' OR imdb ? 'id'))
    );

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_tomatoes_structure 
    CHECK (
        tomatoes IS NULL OR 
        (jsonb_typeof(tomatoes) = 'object' AND 
         (tomatoes ? 'viewer' OR tomatoes ? 'critic' OR tomatoes ? 'fresh' OR tomatoes ? 'rotten'))
    );

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_plot_embedding_structure 
    CHECK (
        plot_embedding IS NULL OR 
        jsonb_typeof(plot_embedding) = 'array'
    );

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_plot_embedding_voyage_structure 
    CHECK (
        plot_embedding_voyage_3_large IS NULL OR 
        jsonb_typeof(plot_embedding_voyage_3_large) = 'array'
    );


ALTER TABLE theaters ADD CONSTRAINT theaters_location_structure 
    CHECK (
        location IS NOT NULL AND 
        jsonb_typeof(location) = 'object' AND 
        (location ? 'geo' OR location ? 'address')
    );

ALTER TABLE theaters ADD CONSTRAINT theaters_address_structure 
    CHECK (
        address IS NOT NULL AND 
        jsonb_typeof(address) = 'object' AND 
        (address ? 'street1' OR address ? 'city' OR address ? 'state' OR address ? 'zipcode')
    );

ALTER TABLE theaters ADD CONSTRAINT theaters_coordinates_bounds 
    CHECK (
        coordinates IS NULL OR 
        (ST_X(coordinates) BETWEEN -180 AND 180 AND 
         ST_Y(coordinates) BETWEEN -90 AND 90)
    );


ALTER TABLE users ADD CONSTRAINT users_preferences_structure 
    CHECK (
        preferences IS NULL OR 
        jsonb_typeof(preferences) = 'object'
    );


ALTER TABLE sessions ADD CONSTRAINT sessions_data_structure 
    CHECK (
        session_data IS NULL OR 
        jsonb_typeof(session_data) = 'object'
    );


ALTER TABLE movies ADD CONSTRAINT movies_genres_not_empty 
    CHECK (genres IS NULL OR array_length(genres, 1) > 0);

ALTER TABLE movies ADD CONSTRAINT movies_cast_not_empty 
    CHECK (cast IS NULL OR array_length(cast, 1) > 0);

ALTER TABLE movies ADD CONSTRAINT movies_directors_not_empty 
    CHECK (directors IS NULL OR array_length(directors, 1) > 0);

ALTER TABLE movies ADD CONSTRAINT movies_languages_not_empty 
    CHECK (languages IS NULL OR array_length(languages, 1) > 0);

ALTER TABLE movies ADD CONSTRAINT movies_countries_not_empty 
    CHECK (countries IS NULL OR array_length(countries, 1) > 0);

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_genres_not_empty 
    CHECK (genres IS NULL OR array_length(genres, 1) > 0);

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_cast_not_empty 
    CHECK (cast IS NULL OR array_length(cast, 1) > 0);

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_directors_not_empty 
    CHECK (directors IS NULL OR array_length(directors, 1) > 0);

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_languages_not_empty 
    CHECK (languages IS NULL OR array_length(languages, 1) > 0);

ALTER TABLE embedded_movies ADD CONSTRAINT embedded_movies_countries_not_empty 
    CHECK (countries IS NULL OR array_length(countries, 1) > 0);







/*
ALTER TABLE sessions ADD CONSTRAINT sessions_no_overlapping_active 
    EXCLUDE USING gist (
        user_id WITH =,
        tstzrange(created_at, expires_at) WITH &&
    ) WHERE (is_active = TRUE);
*/


/*
CREATE UNIQUE INDEX idx_sessions_unique_active_per_user 
    ON sessions(user_id) 
    WHERE is_active = TRUE;
*/


CREATE DOMAIN email_address AS VARCHAR(255)
    CHECK (VALUE ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$');

CREATE DOMAIN movie_rating AS VARCHAR(10)
    CHECK (VALUE IS NULL OR VALUE IN ('G', 'PG', 'PG-13', 'R', 'NC-17', 'NR', 'UNRATED', 'APPROVED', 'NOT RATED', 'PASSED', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA'));

CREATE DOMAIN movie_year AS INTEGER
    CHECK (VALUE IS NULL OR (VALUE >= 1800 AND VALUE <= EXTRACT(YEAR FROM CURRENT_DATE) + 10));

CREATE DOMAIN positive_integer AS INTEGER
    CHECK (VALUE IS NULL OR VALUE > 0);

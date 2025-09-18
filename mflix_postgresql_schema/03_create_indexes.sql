

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users(email);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_name_lower ON users(LOWER(name));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_preferences_gin ON users USING GIN(preferences);


CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sessions_active ON sessions(is_active) WHERE is_active = TRUE;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sessions_jwt_token ON sessions(jwt_token);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sessions_data_gin ON sessions USING GIN(session_data);


CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_theaters_theater_id ON theaters(theater_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_theaters_coordinates_gist ON theaters USING GIST(coordinates);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_theaters_location_gin ON theaters USING GIN(location);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_theaters_address_gin ON theaters USING GIN(address);


CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_title_lower ON movies(LOWER(title));
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_title_trgm ON movies USING GIN(title gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_genres_gin ON movies USING GIN(genres);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_cast_gin ON movies USING GIN(cast);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_directors_gin ON movies USING GIN(directors);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_languages_gin ON movies USING GIN(languages);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_countries_gin ON movies USING GIN(countries);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_year ON movies(year);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_released ON movies(released);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_runtime ON movies(runtime);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_rated ON movies(rated);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_type ON movies(type);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_num_comments ON movies(num_mflix_comments);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_awards_gin ON movies USING GIN(awards);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_imdb_gin ON movies USING GIN(imdb);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_tomatoes_gin ON movies USING GIN(tomatoes);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_plot_embedding_gin ON movies USING GIN(plot_embedding);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_plot_embedding_voyage_gin ON movies USING GIN(plot_embedding_voyage_3_large);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_plot_trgm ON movies USING GIN(plot gin_trgm_ops);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_fullplot_trgm ON movies USING GIN(fullplot gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_year_rating ON movies(year, rated);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_released_runtime ON movies(released, runtime);


CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_movie_id ON embedded_movies(movie_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_title_lower ON embedded_movies(LOWER(title));
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_title_trgm ON embedded_movies USING GIN(title gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_genres_gin ON embedded_movies USING GIN(genres);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_cast_gin ON embedded_movies USING GIN(cast);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_directors_gin ON embedded_movies USING GIN(directors);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_year ON embedded_movies(year);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_rated ON embedded_movies(rated);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_awards_gin ON embedded_movies USING GIN(awards);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_imdb_gin ON embedded_movies USING GIN(imdb);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_tomatoes_gin ON embedded_movies USING GIN(tomatoes);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_plot_embedding_gin ON embedded_movies USING GIN(plot_embedding);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_embedded_movies_plot_embedding_voyage_gin ON embedded_movies USING GIN(plot_embedding_voyage_3_large);


CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_movie_id ON comments(movie_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_user_id ON comments(user_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_email ON comments(email);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_name_lower ON comments(LOWER(name));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_date ON comments(date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_text_trgm ON comments USING GIN(text gin_trgm_ops);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_movie_date ON comments(movie_id, date);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_user_date ON comments(user_id, date);



CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_theaters_coordinates_spgist ON theaters USING SPGIST(coordinates);



CREATE OR REPLACE VIEW index_usage_stats AS
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    idx_scan
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
ORDER BY idx_scan DESC;

CREATE OR REPLACE VIEW table_sizes AS
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY size_bytes DESC;

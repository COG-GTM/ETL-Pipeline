

CREATE TABLE IF NOT EXISTS migration_history (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL UNIQUE,
    executed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    execution_time_ms INTEGER,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    checksum VARCHAR(64)
);

CREATE OR REPLACE FUNCTION log_migration_step(
    step_name VARCHAR(255),
    start_time TIMESTAMP WITH TIME ZONE,
    success_flag BOOLEAN DEFAULT TRUE,
    error_msg TEXT DEFAULT NULL
) RETURNS VOID AS $$
DECLARE
    execution_time INTEGER;
BEGIN
    execution_time := EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - start_time)) * 1000;
    
    INSERT INTO migration_history (migration_name, execution_time_ms, success, error_message)
    VALUES (step_name, execution_time, success_flag, error_msg);
    
    IF success_flag THEN
        RAISE NOTICE 'Migration step completed: % (% ms)', step_name, execution_time;
    ELSE
        RAISE NOTICE 'Migration step failed: % - %', step_name, error_msg;
    END IF;
END;
$$ LANGUAGE plpgsql;


DO $$
DECLARE
    start_time TIMESTAMP WITH TIME ZONE := CURRENT_TIMESTAMP;
BEGIN
    CREATE EXTENSION IF NOT EXISTS postgis;
    
    CREATE EXTENSION IF NOT EXISTS postgis_topology;
    
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    
    CREATE EXTENSION IF NOT EXISTS btree_gin;
    
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
    
    PERFORM log_migration_step('01_enable_extensions', start_time);
EXCEPTION WHEN OTHERS THEN
    PERFORM log_migration_step('01_enable_extensions', start_time, FALSE, SQLERRM);
    RAISE;
END $$;


DO $$
DECLARE
    start_time TIMESTAMP WITH TIME ZONE := CURRENT_TIMESTAMP;
BEGIN
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        password VARCHAR(255) NOT NULL,
        preferences JSONB DEFAULT '{}',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        
        CONSTRAINT users_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
        CONSTRAINT users_name_not_empty CHECK (LENGTH(TRIM(name)) > 0),
        CONSTRAINT users_password_min_length CHECK (LENGTH(password) >= 8)
    );

    CREATE TABLE sessions (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        user_id INTEGER NOT NULL,
        jwt_token TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        session_data JSONB DEFAULT '{}',
        
        CONSTRAINT fk_sessions_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
        
        CONSTRAINT sessions_expires_after_created CHECK (expires_at > created_at),
        CONSTRAINT sessions_jwt_not_empty CHECK (LENGTH(TRIM(jwt_token)) > 0),
        CONSTRAINT sessions_expires_reasonable CHECK (expires_at <= created_at + INTERVAL '30 days'),
        CONSTRAINT sessions_data_structure CHECK (session_data IS NULL OR jsonb_typeof(session_data) = 'object')
    );

    CREATE TABLE theaters (
        id SERIAL PRIMARY KEY,
        theater_id INTEGER UNIQUE NOT NULL,
        location JSONB NOT NULL,
        coordinates GEOMETRY(POINT, 4326),
        address JSONB NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        
        CONSTRAINT theaters_location_required CHECK (location IS NOT NULL),
        CONSTRAINT theaters_address_required CHECK (address IS NOT NULL),
        CONSTRAINT theaters_coordinates_valid CHECK (ST_IsValid(coordinates)),
        CONSTRAINT theaters_location_structure CHECK (
            location IS NOT NULL AND 
            jsonb_typeof(location) = 'object' AND 
            (location ? 'geo' OR location ? 'address')
        ),
        CONSTRAINT theaters_address_structure CHECK (
            address IS NOT NULL AND 
            jsonb_typeof(address) = 'object' AND 
            (address ? 'street1' OR address ? 'city' OR address ? 'state' OR address ? 'zipcode')
        ),
        CONSTRAINT theaters_coordinates_bounds CHECK (
            coordinates IS NULL OR 
            (ST_X(coordinates) BETWEEN -180 AND 180 AND 
             ST_Y(coordinates) BETWEEN -90 AND 90)
        )
    );

    CREATE TABLE movies (
        id SERIAL PRIMARY KEY,
        plot TEXT,
        genres TEXT[] DEFAULT '{}',
        runtime INTEGER,
        cast TEXT[] DEFAULT '{}',
        poster VARCHAR(500),
        title VARCHAR(500) NOT NULL,
        fullplot TEXT,
        languages TEXT[] DEFAULT '{}',
        released DATE,
        directors TEXT[] DEFAULT '{}',
        rated VARCHAR(10),
        
        awards JSONB DEFAULT '{}',
        lastupdated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        year INTEGER,
        imdb JSONB DEFAULT '{}',
        countries TEXT[] DEFAULT '{}',
        type VARCHAR(50),
        tomatoes JSONB DEFAULT '{}',
        num_mflix_comments INTEGER DEFAULT 0,
        
        plot_embedding JSONB,
        plot_embedding_voyage_3_large JSONB,
        
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        
        CONSTRAINT movies_title_not_empty CHECK (LENGTH(TRIM(title)) > 0),
        CONSTRAINT movies_year_valid CHECK (year IS NULL OR (year >= 1800 AND year <= EXTRACT(YEAR FROM CURRENT_DATE) + 10)),
        CONSTRAINT movies_runtime_positive CHECK (runtime IS NULL OR runtime > 0),
        CONSTRAINT movies_num_comments_non_negative CHECK (num_mflix_comments >= 0),
        CONSTRAINT movies_poster_url_format CHECK (poster IS NULL OR poster ~* '^https?://.*\.(jpg|jpeg|png|gif|webp)(\?.*)?$'),
        CONSTRAINT movies_rated_valid_values CHECK (rated IS NULL OR rated IN ('G', 'PG', 'PG-13', 'R', 'NC-17', 'NR', 'UNRATED', 'APPROVED', 'NOT RATED', 'PASSED', 'TV-G', 'TV-PG', 'TV-14', 'TV-MA')),
        CONSTRAINT movies_type_valid_values CHECK (type IS NULL OR type IN ('movie', 'series', 'episode'))
    );

    CREATE TABLE embedded_movies (
        id SERIAL PRIMARY KEY,
        movie_id INTEGER NOT NULL,
        plot TEXT,
        genres TEXT[] DEFAULT '{}',
        runtime INTEGER,
        cast TEXT[] DEFAULT '{}',
        poster VARCHAR(500),
        title VARCHAR(500) NOT NULL,
        fullplot TEXT,
        languages TEXT[] DEFAULT '{}',
        released DATE,
        directors TEXT[] DEFAULT '{}',
        rated VARCHAR(10),
        
        awards JSONB DEFAULT '{}',
        lastupdated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        year INTEGER,
        imdb JSONB DEFAULT '{}',
        countries TEXT[] DEFAULT '{}',
        type VARCHAR(50),
        tomatoes JSONB DEFAULT '{}',
        
        plot_embedding JSONB,
        plot_embedding_voyage_3_large JSONB,
        
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        
        CONSTRAINT fk_embedded_movies_movie_id FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        
        CONSTRAINT embedded_movies_title_not_empty CHECK (LENGTH(TRIM(title)) > 0),
        CONSTRAINT embedded_movies_year_valid CHECK (year IS NULL OR (year >= 1800 AND year <= EXTRACT(YEAR FROM CURRENT_DATE) + 10)),
        CONSTRAINT embedded_movies_runtime_positive CHECK (runtime IS NULL OR runtime > 0)
    );

    CREATE TABLE comments (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        movie_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        user_id INTEGER,
        
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        
        CONSTRAINT fk_comments_movie_id FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
        CONSTRAINT fk_comments_user_id FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
        
        CONSTRAINT comments_name_not_empty CHECK (LENGTH(TRIM(name)) > 0),
        CONSTRAINT comments_text_not_empty CHECK (LENGTH(TRIM(text)) > 0),
        CONSTRAINT comments_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    );

    PERFORM log_migration_step('02_create_tables', start_time);
EXCEPTION WHEN OTHERS THEN
    PERFORM log_migration_step('02_create_tables', start_time, FALSE, SQLERRM);
    RAISE;
END $$;


DO $$
DECLARE
    start_time TIMESTAMP WITH TIME ZONE := CURRENT_TIMESTAMP;
BEGIN
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $func$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $func$ language 'plpgsql';

    CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    CREATE TRIGGER update_theaters_updated_at BEFORE UPDATE ON theaters FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    CREATE TRIGGER update_movies_updated_at BEFORE UPDATE ON movies FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    CREATE TRIGGER update_embedded_movies_updated_at BEFORE UPDATE ON embedded_movies FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    CREATE TRIGGER update_comments_updated_at BEFORE UPDATE ON comments FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

    CREATE OR REPLACE FUNCTION update_movie_comment_count()
    RETURNS TRIGGER AS $func$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            UPDATE movies SET num_mflix_comments = num_mflix_comments + 1 WHERE id = NEW.movie_id;
            RETURN NEW;
        ELSIF TG_OP = 'DELETE' THEN
            UPDATE movies SET num_mflix_comments = num_mflix_comments - 1 WHERE id = OLD.movie_id;
            RETURN OLD;
        ELSIF TG_OP = 'UPDATE' AND OLD.movie_id != NEW.movie_id THEN
            UPDATE movies SET num_mflix_comments = num_mflix_comments - 1 WHERE id = OLD.movie_id;
            UPDATE movies SET num_mflix_comments = num_mflix_comments + 1 WHERE id = NEW.movie_id;
            RETURN NEW;
        END IF;
        RETURN NULL;
    END;
    $func$ language 'plpgsql';

    CREATE TRIGGER update_movie_comment_count_trigger
        AFTER INSERT OR UPDATE OR DELETE ON comments
        FOR EACH ROW EXECUTE FUNCTION update_movie_comment_count();

    PERFORM log_migration_step('03_create_functions_triggers', start_time);
EXCEPTION WHEN OTHERS THEN
    PERFORM log_migration_step('03_create_functions_triggers', start_time, FALSE, SQLERRM);
    RAISE;
END $$;


DO $$
DECLARE
    start_time TIMESTAMP WITH TIME ZONE := CURRENT_TIMESTAMP;
BEGIN
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
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_year ON movies(year);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_rated ON movies(rated);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_awards_gin ON movies USING GIN(awards);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_imdb_gin ON movies USING GIN(imdb);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_tomatoes_gin ON movies USING GIN(tomatoes);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_plot_embedding_gin ON movies USING GIN(plot_embedding);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_movies_plot_embedding_voyage_gin ON movies USING GIN(plot_embedding_voyage_3_large);

    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_movie_id ON comments(movie_id);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_user_id ON comments(user_id);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_date ON comments(date);
    CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_comments_text_trgm ON comments USING GIN(text gin_trgm_ops);

    PERFORM log_migration_step('04_create_indexes', start_time);
EXCEPTION WHEN OTHERS THEN
    PERFORM log_migration_step('04_create_indexes', start_time, FALSE, SQLERRM);
    RAISE;
END $$;


DO $$
DECLARE
    start_time TIMESTAMP WITH TIME ZONE := CURRENT_TIMESTAMP;
BEGIN
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

    PERFORM log_migration_step('05_create_monitoring_views', start_time);
EXCEPTION WHEN OTHERS THEN
    PERFORM log_migration_step('05_create_monitoring_views', start_time, FALSE, SQLERRM);
    RAISE;
END $$;


DO $$
BEGIN
    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'MFLIX PostgreSQL Migration Completed Successfully!';
    RAISE NOTICE '=================================================================';
    RAISE NOTICE 'Tables created: users, sessions, theaters, movies, embedded_movies, comments';
    RAISE NOTICE 'Extensions enabled: postgis, postgis_topology, uuid-ossp, btree_gin, pg_trgm';
    RAISE NOTICE 'Indexes created: Performance-optimized indexes including spatial indexes';
    RAISE NOTICE 'Triggers created: Automatic timestamp updates and comment counting';
    RAISE NOTICE 'Constraints: Data integrity and JSONB structure validation';
    RAISE NOTICE '=================================================================';
    
    RAISE NOTICE 'Migration Summary:';
    FOR rec IN SELECT migration_name, execution_time_ms, success FROM migration_history ORDER BY id LOOP
        RAISE NOTICE '  % - % ms - %', rec.migration_name, rec.execution_time_ms, 
                     CASE WHEN rec.success THEN 'SUCCESS' ELSE 'FAILED' END;
    END LOOP;
END $$;

SELECT 
    extname as "Extension Name",
    extversion as "Version"
FROM pg_extension 
WHERE extname IN ('postgis', 'postgis_topology', 'uuid-ossp', 'btree_gin', 'pg_trgm')
ORDER BY extname;

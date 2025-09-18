
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    preferences JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT users_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
    CONSTRAINT users_name_not_empty CHECK (LENGTH(TRIM(name)) > 0)
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
    CONSTRAINT sessions_jwt_not_empty CHECK (LENGTH(TRIM(jwt_token)) > 0)
);

CREATE TABLE theaters (
    id SERIAL PRIMARY KEY,
    theater_id INTEGER UNIQUE NOT NULL,
    location JSONB NOT NULL,
    coordinates GEOMETRY(POINT, 4326), -- PostGIS point geometry (WGS84)
    address JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT theaters_location_required CHECK (location IS NOT NULL),
    CONSTRAINT theaters_address_required CHECK (address IS NOT NULL),
    CONSTRAINT theaters_coordinates_valid CHECK (ST_IsValid(coordinates))
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
    CONSTRAINT movies_num_comments_non_negative CHECK (num_mflix_comments >= 0)
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


CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_theaters_updated_at BEFORE UPDATE ON theaters FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_movies_updated_at BEFORE UPDATE ON movies FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_embedded_movies_updated_at BEFORE UPDATE ON embedded_movies FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_comments_updated_at BEFORE UPDATE ON comments FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


CREATE OR REPLACE FUNCTION update_movie_comment_count()
RETURNS TRIGGER AS $$
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
$$ language 'plpgsql';

CREATE TRIGGER update_movie_comment_count_trigger
    AFTER INSERT OR UPDATE OR DELETE ON comments
    FOR EACH ROW EXECUTE FUNCTION update_movie_comment_count();

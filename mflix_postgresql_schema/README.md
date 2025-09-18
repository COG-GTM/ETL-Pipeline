# MFLIX PostgreSQL Schema Migration

This directory contains the PostgreSQL schema design and migration scripts for converting the MFLIX MongoDB models to PostgreSQL.

## Overview

Converting 6 MongoDB models to PostgreSQL:
1. **Movie** - Core movie information with JSONB for complex nested data
2. **EmbeddedMovie** - Embedded movie data with awards, IMDB, and Tomatoes objects
3. **Comment** - User comments linked to movies
4. **User** - User accounts with authentication data
5. **Session** - User session management
6. **Theater** - Theater locations with PostGIS geospatial data

## Key Design Decisions

### JSONB Usage
- **Awards, IMDB, Tomatoes**: Stored as JSONB for flexible nested object storage
- **AI Embeddings**: JSONB columns for `plot_embedding` and `plot_embedding_voyage_3_large`
- **Metadata**: Additional flexible metadata stored as JSONB

### PostGIS Integration
- **Theater Coordinates**: Using PostGIS POINT geometry for precise location data
- **Spatial Indexes**: GiST indexes for efficient geospatial queries

### Normalization Strategy
- **Comments**: Separate table with foreign key to movies
- **Sessions**: Separate table linked to users
- **Movies**: Core table with JSONB for complex nested data rather than full normalization

## Files

- `01_enable_extensions.sql` - Enable PostGIS and other required extensions
- `02_create_tables.sql` - Main table creation DDL
- `03_create_indexes.sql` - Index creation including spatial indexes
- `04_create_constraints.sql` - Additional constraints and foreign keys
- `migration_script.sql` - Complete migration script with proper sequencing
- `connection_pool_config.sql` - Connection pooling configuration

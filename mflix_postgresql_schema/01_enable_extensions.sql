
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE EXTENSION IF NOT EXISTS postgis_topology;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE EXTENSION IF NOT EXISTS btree_gin;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

SELECT 
    extname as "Extension Name",
    extversion as "Version"
FROM pg_extension 
WHERE extname IN ('postgis', 'postgis_topology', 'uuid-ossp', 'btree_gin', 'pg_trgm')
ORDER BY extname;

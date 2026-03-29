

















/*
[databases]
mflix = host=localhost port=5432 dbname=mflix user=mflix_app password=your_password

[pgbouncer]
listen_port = 6432
listen_addr = *
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt

# Connection pool settings
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
min_pool_size = 5
reserve_pool_size = 5
reserve_pool_timeout = 5

# Server connection settings
server_reset_query = DISCARD ALL
server_check_query = SELECT 1
server_check_delay = 30
max_db_connections = 50
max_user_connections = 50

# Timeouts
server_connect_timeout = 15
server_login_retry = 15
query_timeout = 0
query_wait_timeout = 120
client_idle_timeout = 0
client_login_timeout = 60
autodb_idle_timeout = 3600

# Logging
admin_users = pgbouncer_admin
stats_users = pgbouncer_stats
log_connections = 1
log_disconnections = 1
log_pooler_errors = 1

# Security
ignore_startup_parameters = extra_float_digits
*/



/*
Application Connection Pool Configuration:

1. Maximum Pool Size: 10-20 connections per application instance
2. Minimum Pool Size: 2-5 connections
3. Connection Timeout: 30 seconds
4. Idle Timeout: 10 minutes
5. Max Lifetime: 30 minutes
6. Validation Query: "SELECT 1"
7. Test on Borrow: true
8. Test While Idle: true

Example for HikariCP (Java):
- maximumPoolSize: 20
- minimumIdle: 5
- connectionTimeout: 30000
- idleTimeout: 600000
- maxLifetime: 1800000
- connectionTestQuery: "SELECT 1"
*/


CREATE OR REPLACE VIEW connection_pool_stats AS
SELECT 
    state,
    COUNT(*) as connection_count,
    MAX(state_change) as last_state_change
FROM pg_stat_activity 
WHERE pid != pg_backend_pid()
GROUP BY state
ORDER BY connection_count DESC;

CREATE OR REPLACE VIEW active_connections AS
SELECT 
    datname as database,
    usename as username,
    client_addr,
    state,
    query_start,
    state_change,
    EXTRACT(EPOCH FROM (now() - query_start)) as query_duration_seconds
FROM pg_stat_activity 
WHERE pid != pg_backend_pid()
  AND state != 'idle'
ORDER BY query_start;

CREATE OR REPLACE VIEW long_running_queries AS
SELECT 
    pid,
    usename,
    datname,
    query_start,
    state,
    EXTRACT(EPOCH FROM (now() - query_start)) as duration_seconds,
    LEFT(query, 100) as query_preview
FROM pg_stat_activity 
WHERE pid != pg_backend_pid()
  AND state != 'idle'
  AND query_start < now() - interval '30 seconds'
ORDER BY query_start;


CREATE OR REPLACE FUNCTION kill_idle_connections(idle_threshold INTERVAL DEFAULT '1 hour')
RETURNS INTEGER AS $$
DECLARE
    killed_count INTEGER := 0;
    conn_record RECORD;
BEGIN
    FOR conn_record IN 
        SELECT pid, usename, datname, state_change
        FROM pg_stat_activity 
        WHERE state = 'idle'
          AND state_change < now() - idle_threshold
          AND pid != pg_backend_pid()
          AND usename != 'postgres'  -- Don't kill superuser connections
    LOOP
        BEGIN
            PERFORM pg_terminate_backend(conn_record.pid);
            killed_count := killed_count + 1;
            RAISE NOTICE 'Killed idle connection: PID %, User %, DB %, Idle since %', 
                         conn_record.pid, conn_record.usename, conn_record.datname, conn_record.state_change;
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'Failed to kill connection PID %: %', conn_record.pid, SQLERRM;
        END;
    END LOOP;
    
    RETURN killed_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_connection_pool_recommendations()
RETURNS TABLE(
    metric VARCHAR(50),
    current_value TEXT,
    recommended_value TEXT,
    description TEXT
) AS $$
DECLARE
    total_connections INTEGER;
    active_connections INTEGER;
    idle_connections INTEGER;
    max_conn_setting INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_connections FROM pg_stat_activity WHERE pid != pg_backend_pid();
    SELECT COUNT(*) INTO active_connections FROM pg_stat_activity WHERE state = 'active' AND pid != pg_backend_pid();
    SELECT COUNT(*) INTO idle_connections FROM pg_stat_activity WHERE state = 'idle' AND pid != pg_backend_pid();
    SELECT setting::INTEGER INTO max_conn_setting FROM pg_settings WHERE name = 'max_connections';
    
    RETURN QUERY VALUES
        ('Total Connections', total_connections::TEXT, 'Monitor < 80% of max_connections', 'Current active connections'),
        ('Max Connections', max_conn_setting::TEXT, '200-400 for most apps', 'PostgreSQL max_connections setting'),
        ('Active Connections', active_connections::TEXT, 'Should be < 50% of total', 'Currently executing queries'),
        ('Idle Connections', idle_connections::TEXT, 'Should be minimal with pooling', 'Idle but connected'),
        ('Connection Utilization', ROUND((total_connections::NUMERIC / max_conn_setting::NUMERIC) * 100, 2)::TEXT || '%', '< 80%', 'Percentage of max connections used');
END;
$$ LANGUAGE plpgsql;



/*


*/











/*
Connection Pool Performance Tips:

1. Use transaction-level pooling (pgbouncer) for better connection reuse
2. Monitor connection pool metrics regularly
3. Adjust pool sizes based on actual load patterns
4. Use prepared statements to reduce parsing overhead
5. Implement connection validation to handle network issues
6. Set appropriate timeouts to prevent resource leaks
7. Use read replicas for read-heavy workloads
8. Consider connection multiplexing for high-concurrency applications

Database-level optimizations:
1. Tune shared_buffers, work_mem, and effective_cache_size
2. Enable connection compression for remote connections
3. Use SSL/TLS for secure connections in production
4. Monitor and optimize slow queries
5. Implement proper indexing strategies
6. Use VACUUM and ANALYZE regularly
7. Consider partitioning for large tables
8. Implement proper backup and recovery procedures
*/

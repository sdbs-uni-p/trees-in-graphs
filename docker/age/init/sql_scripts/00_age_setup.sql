CREATE EXTENSION IF NOT EXISTS age;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

DO $$
BEGIN
  EXECUTE format(
    'ALTER DATABASE %I SET search_path = ag_catalog, "$user", public',
    current_database()
  );
END $$;

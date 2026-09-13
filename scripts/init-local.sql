-- Local Compose development only. Never use these credentials in a deployment.
CREATE ROLE runtime_local LOGIN PASSWORD 'local-only' NOSUPERUSER NOBYPASSRLS;

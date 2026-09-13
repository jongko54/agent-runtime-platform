"""Frozen initial schema; runtime metadata is not imported by migrations."""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(SCHEMA)
    op.execute(SECURITY)


def downgrade() -> None:
    op.execute("DROP FUNCTION public.claim_runtime_work()")
    op.execute(
        "DROP TABLE usage_entries,tool_calls,model_calls,idempotency_records,work_items,"
        "run_events,run_steps,runs,tool_versions,agent_versions,tool_definitions,"
        "agent_definitions,connections,project_memberships,principals,projects,tenants"
    )
    op.execute("DROP FUNCTION public.reject_version_mutation()")
    # Cluster roles may serve other databases; never remove them during schema rollback.


SCHEMA = """
CREATE TABLE tenants (id varchar(64) PRIMARY KEY, status varchar(32) NOT NULL);
CREATE TABLE projects (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL REFERENCES tenants(id),
 name varchar(200) NOT NULL,status varchar(32) NOT NULL,UNIQUE(tenant_id,id)
);
CREATE TABLE principals (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL REFERENCES tenants(id),
 issuer varchar(500) NOT NULL,subject varchar(500) NOT NULL,type varchar(32) NOT NULL,
 status varchar(32) NOT NULL,UNIQUE(tenant_id,id),UNIQUE(tenant_id,issuer,subject)
);
CREATE TABLE project_memberships (
 tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,principal_id varchar(64) NOT NULL,
 role_set_id varchar(64) NOT NULL,status varchar(32) NOT NULL,
 PRIMARY KEY(tenant_id,project_id,principal_id),
 FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id),
 FOREIGN KEY(tenant_id,principal_id) REFERENCES principals(tenant_id,id)
);
CREATE TABLE connections (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 kind varchar(100) NOT NULL,config_ref varchar(500) NOT NULL,credential_ref varchar(500),
 status varchar(32) NOT NULL,UNIQUE(tenant_id,project_id,id),
 FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id)
);
CREATE TABLE agent_definitions (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 name varchar(200) NOT NULL,UNIQUE(tenant_id,project_id,id),UNIQUE(tenant_id,project_id,name),
 FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id)
);
CREATE TABLE tool_definitions (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 name varchar(200) NOT NULL,UNIQUE(tenant_id,project_id,id),UNIQUE(tenant_id,project_id,name),
 FOREIGN KEY(tenant_id,project_id) REFERENCES projects(tenant_id,id)
);
CREATE TABLE agent_versions (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 definition_id varchar(64) NOT NULL,version integer NOT NULL CHECK(version>0),
 digest varchar(64) NOT NULL,spec jsonb NOT NULL,
 UNIQUE(tenant_id,project_id,id),UNIQUE(tenant_id,project_id,definition_id,version),
 FOREIGN KEY(tenant_id,project_id,definition_id)
 REFERENCES agent_definitions(tenant_id,project_id,id)
);
CREATE TABLE tool_versions (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 definition_id varchar(64) NOT NULL,name varchar(200) NOT NULL,
 version integer NOT NULL CHECK(version>0),
 schema_digest varchar(64) NOT NULL,input_schema jsonb NOT NULL,output_schema jsonb NOT NULL,
 risk_tier varchar(8) NOT NULL,connection_kind varchar(100) NOT NULL,
 UNIQUE(tenant_id,project_id,id),UNIQUE(tenant_id,project_id,definition_id,version),
 UNIQUE(tenant_id,project_id,name,version),
 FOREIGN KEY(tenant_id,project_id,definition_id)
 REFERENCES tool_definitions(tenant_id,project_id,id)
);
CREATE TABLE runs (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 principal_id varchar(64) NOT NULL,agent_version_id varchar(64) NOT NULL,state varchar(32) NOT NULL,
 state_version bigint NOT NULL CHECK(state_version>0),input jsonb NOT NULL,result jsonb,error jsonb,
 created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(tenant_id,project_id,id),
 FOREIGN KEY(tenant_id,project_id,agent_version_id)
 REFERENCES agent_versions(tenant_id,project_id,id),
 FOREIGN KEY(tenant_id,principal_id) REFERENCES principals(tenant_id,id)
);
CREATE TABLE run_steps (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 run_id varchar(64) NOT NULL,ordinal integer NOT NULL,
 kind varchar(32) NOT NULL,state varchar(32) NOT NULL,
 input jsonb NOT NULL,output jsonb,
 UNIQUE(tenant_id,project_id,run_id,id),UNIQUE(tenant_id,project_id,run_id,ordinal),
 FOREIGN KEY(tenant_id,project_id,run_id) REFERENCES runs(tenant_id,project_id,id)
);
CREATE TABLE run_events (
 tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,run_id varchar(64) NOT NULL,
 sequence bigint NOT NULL,type varchar(100) NOT NULL,schema_version integer NOT NULL,
 actor varchar(200) NOT NULL,payload jsonb NOT NULL,occurred_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,project_id,run_id,sequence),
 FOREIGN KEY(tenant_id,project_id,run_id) REFERENCES runs(tenant_id,project_id,id)
);
CREATE TABLE work_items (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 run_id varchar(64) NOT NULL,step_id varchar(64) NOT NULL,status varchar(32) NOT NULL,
 available_at timestamptz NOT NULL DEFAULT now(),UNIQUE(tenant_id,project_id,run_id,step_id),
 FOREIGN KEY(tenant_id,project_id,run_id,step_id)
 REFERENCES run_steps(tenant_id,project_id,run_id,id)
);
CREATE TABLE idempotency_records (
 tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,principal_id varchar(64) NOT NULL,
 key varchar(200) NOT NULL,request_hash varchar(64) NOT NULL,run_id varchar(64) NOT NULL,
 PRIMARY KEY(tenant_id,project_id,principal_id,key),
 FOREIGN KEY(tenant_id,project_id,run_id) REFERENCES runs(tenant_id,project_id,id)
 DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(tenant_id,principal_id) REFERENCES principals(tenant_id,id)
);
CREATE TABLE model_calls (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 run_id varchar(64) NOT NULL,step_id varchar(64) NOT NULL,model_route varchar(200) NOT NULL,
 status varchar(32) NOT NULL,response jsonb,
 UNIQUE(tenant_id,project_id,run_id,step_id),
 FOREIGN KEY(tenant_id,project_id,run_id,step_id)
 REFERENCES run_steps(tenant_id,project_id,run_id,id)
);
CREATE TABLE tool_calls (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 run_id varchar(64) NOT NULL,step_id varchar(64) NOT NULL,tool_version_id varchar(64) NOT NULL,
 status varchar(32) NOT NULL,arguments jsonb NOT NULL,result jsonb,
 UNIQUE(tenant_id,project_id,run_id,step_id),
 FOREIGN KEY(tenant_id,project_id,run_id,step_id)
 REFERENCES run_steps(tenant_id,project_id,run_id,id),
 FOREIGN KEY(tenant_id,project_id,tool_version_id) REFERENCES tool_versions(tenant_id,project_id,id)
);
CREATE TABLE usage_entries (
 id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
 run_id varchar(64) NOT NULL,step_id varchar(64) NOT NULL,source varchar(32) NOT NULL,
 quantity bigint NOT NULL CHECK(quantity>=0),unit varchar(32) NOT NULL,
 UNIQUE(tenant_id,project_id,run_id,step_id,source,unit),
 FOREIGN KEY(tenant_id,project_id,run_id,step_id)
 REFERENCES run_steps(tenant_id,project_id,run_id,id)
);
CREATE INDEX ix_work_items_ready ON work_items(available_at,id) WHERE status='READY';
CREATE INDEX ix_runs_scope_created ON runs(tenant_id,project_id,created_at);
CREATE FUNCTION public.reject_version_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Published versions are immutable'; END $$;
CREATE TRIGGER agent_version_immutable BEFORE UPDATE OR DELETE ON agent_versions
FOR EACH ROW EXECUTE FUNCTION public.reject_version_mutation();
CREATE TRIGGER tool_version_immutable BEFORE UPDATE OR DELETE ON tool_versions
FOR EACH ROW EXECUTE FUNCTION public.reject_version_mutation();
"""

SECURITY = """
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agent_app') THEN
  CREATE ROLE agent_app NOLOGIN NOSUPERUSER NOBYPASSRLS;
 END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='agent_dispatcher') THEN
  CREATE ROLE agent_dispatcher NOLOGIN NOSUPERUSER NOBYPASSRLS;
 END IF;
END $$;
GRANT USAGE ON SCHEMA public TO agent_app,agent_dispatcher;
GRANT SELECT ON tenants,projects,principals,project_memberships,connections,
 agent_definitions,tool_definitions,agent_versions,tool_versions TO agent_app;
GRANT SELECT,INSERT,UPDATE ON runs,run_steps,work_items,idempotency_records,
 model_calls,tool_calls,usage_entries TO agent_app;
GRANT SELECT,INSERT ON run_events TO agent_app;
GRANT SELECT ON runs TO agent_dispatcher;
GRANT SELECT,UPDATE ON work_items TO agent_dispatcher;
DO $$ DECLARE relation text; BEGIN
 FOREACH relation IN ARRAY ARRAY['projects','principals','project_memberships','connections',
 'agent_definitions','tool_definitions','agent_versions','tool_versions','runs','run_steps',
 'run_events','work_items','idempotency_records','model_calls','tool_calls','usage_entries'] LOOP
  EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY',relation);
  EXECUTE format('ALTER TABLE public.%I FORCE ROW LEVEL SECURITY',relation);
  EXECUTE format('CREATE POLICY tenant_scope ON public.%I TO agent_app '
   'USING (tenant_id = current_setting(''app.tenant_id'',true)) '
   'WITH CHECK (tenant_id = current_setting(''app.tenant_id'',true))',relation);
 END LOOP;
END $$;
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_self ON tenants TO agent_app
 USING(id=current_setting('app.tenant_id',true));
CREATE POLICY dispatch_work ON work_items TO agent_dispatcher USING(true) WITH CHECK(true);
CREATE POLICY dispatch_runs ON runs TO agent_dispatcher USING(true);
CREATE FUNCTION public.claim_runtime_work() RETURNS TABLE(work_id varchar,scope_tenant varchar)
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
 UPDATE public.work_items AS w SET status='PROCESSING'
 WHERE w.id=(SELECT q.id FROM public.work_items AS q
  JOIN public.runs AS r ON r.id=q.run_id AND r.tenant_id=q.tenant_id AND r.project_id=q.project_id
  WHERE q.status='READY' AND q.available_at<=now() AND r.state='QUEUED'
  ORDER BY q.available_at,q.id FOR UPDATE OF q SKIP LOCKED LIMIT 1)
 RETURNING w.id,w.tenant_id
$$;
ALTER FUNCTION public.claim_runtime_work() OWNER TO agent_dispatcher;
REVOKE ALL ON FUNCTION public.claim_runtime_work() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.claim_runtime_work() TO agent_app;
REVOKE ALL ON FUNCTION public.reject_version_mutation() FROM PUBLIC;
"""

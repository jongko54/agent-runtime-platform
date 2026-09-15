"""Append-only, explicitly curated evaluation cases; no runtime replay."""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    ALTER TABLE evaluation_candidates ADD CONSTRAINT uq_candidate_case_source
      UNIQUE(tenant_id,project_id,run_id,id,snapshot_digest);
    ALTER TABLE runs ADD CONSTRAINT uq_run_case_agent
      UNIQUE(tenant_id,project_id,id,agent_version_id);
    CREATE TABLE evaluation_cases (
      id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
      run_id varchar(64) NOT NULL,candidate_id varchar(64) NOT NULL,
      principal_id varchar(64) NOT NULL,source_agent_version_id varchar(64) NOT NULL,
      source_snapshot_digest varchar(64) NOT NULL CHECK(source_snapshot_digest ~ '^[0-9a-f]{64}$'),
      content_digest varchar(64) NOT NULL CHECK(content_digest ~ '^[0-9a-f]{64}$'),
      input jsonb NOT NULL CHECK(jsonb_typeof(input)='object'),
      expected_decision jsonb NOT NULL CHECK(jsonb_typeof(expected_decision)='object'),
      allowed_tools jsonb NOT NULL CHECK(jsonb_typeof(allowed_tools)='array'),
      review_policy varchar(64) NOT NULL CHECK(review_policy='explicit-curation-v1'),
      idempotency_key varchar(200) NOT NULL CHECK(length(btrim(idempotency_key))>0),
      created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      UNIQUE(tenant_id,project_id,principal_id,idempotency_key),
      FOREIGN KEY(tenant_id,project_id,run_id,candidate_id,source_snapshot_digest)
        REFERENCES evaluation_candidates(tenant_id,project_id,run_id,id,snapshot_digest),
      FOREIGN KEY(tenant_id,project_id,run_id,source_agent_version_id)
        REFERENCES runs(tenant_id,project_id,id,agent_version_id),
      FOREIGN KEY(tenant_id,project_id,source_agent_version_id)
        REFERENCES agent_versions(tenant_id,project_id,id),
      FOREIGN KEY(tenant_id,principal_id) REFERENCES principals(tenant_id,id)
    );
    CREATE INDEX ix_evaluation_cases_source
      ON evaluation_cases(tenant_id,project_id,candidate_id);
    GRANT SELECT,INSERT ON evaluation_cases TO agent_app;
    ALTER TABLE evaluation_cases ENABLE ROW LEVEL SECURITY;
    ALTER TABLE evaluation_cases FORCE ROW LEVEL SECURITY;
    CREATE POLICY tenant_scope ON evaluation_cases TO agent_app
      USING(tenant_id=current_setting('app.tenant_id',true))
      WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
    CREATE FUNCTION public.reject_evaluation_case_mutation()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'Evaluation cases are immutable curated records'; END $$;
    CREATE TRIGGER evaluation_case_immutable BEFORE UPDATE OR DELETE ON evaluation_cases
      FOR EACH ROW EXECUTE FUNCTION public.reject_evaluation_case_mutation();
    """)


def downgrade() -> None:
    op.execute("""
    LOCK TABLE evaluation_cases IN ACCESS EXCLUSIVE MODE;
    DO $$ BEGIN
      IF EXISTS(SELECT 1 FROM evaluation_cases) THEN
        RAISE EXCEPTION 'Preserve evaluation cases; downgrade is not permitted';
      END IF;
    END $$;
    DROP TABLE evaluation_cases;
    DROP FUNCTION public.reject_evaluation_case_mutation();
    ALTER TABLE runs DROP CONSTRAINT uq_run_case_agent;
    ALTER TABLE evaluation_candidates DROP CONSTRAINT uq_candidate_case_source;
    """)

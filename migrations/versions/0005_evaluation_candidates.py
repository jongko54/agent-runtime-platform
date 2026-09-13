"""Immutable metadata-only DRAFT evaluation candidates with scoped provenance."""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE evaluation_candidates (
      id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
      run_id varchar(64) NOT NULL,principal_id varchar(64) NOT NULL,
      source_state_version bigint NOT NULL CHECK(source_state_version>0),
      expected_state varchar(32) NOT NULL CHECK(expected_state IN
        ('COMPLETED','FAILED','CANCELLED','TIMED_OUT','REJECTED')),
      status varchar(32) NOT NULL DEFAULT 'DRAFT' CHECK(status='DRAFT'),
      snapshot jsonb NOT NULL CHECK(jsonb_typeof(snapshot)='object'),
      snapshot_digest varchar(64) NOT NULL CHECK(length(snapshot_digest)=64),
      redaction_policy varchar(64) NOT NULL CHECK(redaction_policy='metadata-only-v1'),
      idempotency_key varchar(200) NOT NULL CHECK(length(idempotency_key)>0),
      created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      UNIQUE(tenant_id,project_id,principal_id,idempotency_key),
      FOREIGN KEY(tenant_id,project_id,run_id) REFERENCES runs(tenant_id,project_id,id),
      FOREIGN KEY(tenant_id,principal_id) REFERENCES principals(tenant_id,id)
    );
    CREATE INDEX ix_evaluation_candidates_source
      ON evaluation_candidates(tenant_id,project_id,run_id,source_state_version);
    GRANT SELECT,INSERT ON evaluation_candidates TO agent_app;
    ALTER TABLE evaluation_candidates ENABLE ROW LEVEL SECURITY;
    ALTER TABLE evaluation_candidates FORCE ROW LEVEL SECURITY;
    CREATE POLICY tenant_scope ON evaluation_candidates TO agent_app
      USING(tenant_id=current_setting('app.tenant_id',true))
      WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
    CREATE FUNCTION public.reject_evaluation_candidate_mutation()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'Evaluation candidates are immutable DRAFT records'; END $$;
    CREATE TRIGGER evaluation_candidate_immutable BEFORE UPDATE OR DELETE ON evaluation_candidates
      FOR EACH ROW EXECUTE FUNCTION public.reject_evaluation_candidate_mutation();
    """)


def downgrade() -> None:
    op.execute("""
    LOCK TABLE evaluation_candidates IN ACCESS EXCLUSIVE MODE;
    DO $$ BEGIN
      IF EXISTS(SELECT 1 FROM evaluation_candidates) THEN
        RAISE EXCEPTION 'Preserve evaluation candidates; downgrade is not permitted';
      END IF;
    END $$;
    DROP TABLE evaluation_candidates;
    DROP FUNCTION public.reject_evaluation_candidate_mutation();
    """)

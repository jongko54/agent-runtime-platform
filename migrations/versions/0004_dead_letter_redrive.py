"""Append-only links for explicit, authorized dead-letter full-run restarts."""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    ALTER TABLE dead_letter_items ADD CONSTRAINT dead_letter_redrive_scope
      UNIQUE(tenant_id,project_id,run_id,id);
    CREATE TABLE dead_letter_redrives (
      id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
      source_run_id varchar(64) NOT NULL,source_dead_letter_id varchar(64) NOT NULL UNIQUE,
      new_run_id varchar(64) NOT NULL UNIQUE,principal_id varchar(64) NOT NULL,
      idempotency_key varchar(200) NOT NULL CHECK(length(idempotency_key)>0),
      reason varchar(64) NOT NULL CHECK(reason IN
        ('WORKER_RECOVERED','TRANSIENT_FAILURE_RESOLVED')),
      created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      UNIQUE(tenant_id,project_id,principal_id,idempotency_key),
      FOREIGN KEY(tenant_id,project_id,source_run_id,source_dead_letter_id)
        REFERENCES dead_letter_items(tenant_id,project_id,run_id,id),
      FOREIGN KEY(tenant_id,project_id,new_run_id) REFERENCES runs(tenant_id,project_id,id),
      FOREIGN KEY(tenant_id,principal_id) REFERENCES principals(tenant_id,id),
      CHECK(source_run_id<>new_run_id)
    );
    GRANT SELECT,INSERT ON dead_letter_redrives TO agent_app;
    ALTER TABLE dead_letter_redrives ENABLE ROW LEVEL SECURITY;
    ALTER TABLE dead_letter_redrives FORCE ROW LEVEL SECURITY;
    CREATE POLICY tenant_scope ON dead_letter_redrives TO agent_app
      USING(tenant_id=current_setting('app.tenant_id',true))
      WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
    CREATE FUNCTION public.reject_redrive_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'Dead-letter redrive history is immutable'; END $$;
    CREATE TRIGGER redrive_immutable BEFORE UPDATE OR DELETE ON dead_letter_redrives
      FOR EACH ROW EXECUTE FUNCTION public.reject_redrive_mutation();
    """)


def downgrade() -> None:
    op.execute("""
    LOCK TABLE dead_letter_redrives IN ACCESS EXCLUSIVE MODE;
    DO $$ BEGIN
      IF EXISTS(SELECT 1 FROM dead_letter_redrives) THEN
        RAISE EXCEPTION 'Preserve redrive history; downgrade is not permitted';
      END IF;
    END $$;
    DROP TABLE dead_letter_redrives;
    DROP FUNCTION public.reject_redrive_mutation();
    ALTER TABLE dead_letter_items DROP CONSTRAINT dead_letter_redrive_scope;
    """)

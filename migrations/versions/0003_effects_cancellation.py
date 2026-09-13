"""Stable tool effects, cancellation fencing and a durable mock provider simulator."""

import hashlib
import json

from alembic import op
from sqlalchemy import text

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    LOCK TABLE public.work_items IN ACCESS EXCLUSIVE MODE;
    DO $$ BEGIN
      IF EXISTS(SELECT 1 FROM public.work_items WHERE status='PROCESSING')
        OR EXISTS(SELECT 1 FROM public.tool_calls WHERE status IN
          ('DISPATCHED','OUTCOME_UNKNOWN')) THEN
        RAISE EXCEPTION 'Drain PROCESSING work and resolve uncertain calls before upgrading';
      END IF;
    END $$;
    ALTER TABLE runs ADD COLUMN cancel_epoch bigint NOT NULL DEFAULT 0 CHECK(cancel_epoch>=0),
      ADD COLUMN cancellation_outcome varchar(32)
        CHECK(cancellation_outcome IN ('NO_EFFECT','OUTCOME_UNKNOWN','EFFECT_SUCCEEDED'));
    ALTER TABLE run_attempts ADD CONSTRAINT attempt_effect_scope
      UNIQUE(tenant_id,project_id,run_id,step_id,id);
    CREATE TABLE tool_effects (
      id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
      run_id varchar(64) NOT NULL,step_id varchar(64) NOT NULL,tool_version_id varchar(64) NOT NULL,
      tool_version varchar(200) NOT NULL,idempotency_key varchar(200) NOT NULL UNIQUE,
      request_hash varchar(64) NOT NULL,arguments jsonb NOT NULL,result jsonb,
      status varchar(32) NOT NULL CHECK(status IN
        ('PREPARED','DISPATCHED','SUCCEEDED','OUTCOME_UNKNOWN','CANCELLED')),
      dispatch_token varchar(64),dispatch_attempt_id varchar(64),
      created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      UNIQUE(tenant_id,project_id,run_id,step_id),
      FOREIGN KEY(tenant_id,project_id,run_id,step_id)
        REFERENCES run_steps(tenant_id,project_id,run_id,id),
      FOREIGN KEY(tenant_id,project_id,tool_version_id)
        REFERENCES tool_versions(tenant_id,project_id,id),
      FOREIGN KEY(tenant_id,project_id,run_id,step_id,dispatch_attempt_id)
        REFERENCES run_attempts(tenant_id,project_id,run_id,step_id,id),
      CHECK(status NOT IN ('DISPATCHED','SUCCEEDED','OUTCOME_UNKNOWN') OR
        (dispatch_token IS NOT NULL AND dispatch_attempt_id IS NOT NULL))
    );
    CREATE TABLE mock_provider_results (
      idempotency_key varchar(200) PRIMARY KEY,tenant_id varchar(64) NOT NULL,
      project_id varchar(64) NOT NULL,request_hash varchar(64) NOT NULL,result jsonb NOT NULL,
      created_at timestamptz NOT NULL DEFAULT clock_timestamp()
    );
    GRANT SELECT,INSERT,UPDATE ON tool_effects TO agent_app;
    GRANT SELECT,INSERT ON mock_provider_results TO agent_app;
    ALTER TABLE tool_effects ENABLE ROW LEVEL SECURITY;
    ALTER TABLE tool_effects FORCE ROW LEVEL SECURITY;
    CREATE POLICY tenant_scope ON tool_effects TO agent_app
      USING(tenant_id=current_setting('app.tenant_id',true))
      WITH CHECK(tenant_id=current_setting('app.tenant_id',true));
    ALTER TABLE mock_provider_results ENABLE ROW LEVEL SECURITY;
    ALTER TABLE mock_provider_results FORCE ROW LEVEL SECURITY;
    CREATE POLICY provider_scope ON mock_provider_results TO agent_app
      USING(tenant_id=current_setting('app.tenant_id',true)
        AND project_id=current_setting('app.project_id',true))
      WITH CHECK(tenant_id=current_setting('app.tenant_id',true)
        AND project_id=current_setting('app.project_id',true));
    """)
    # Freeze serialization here: migrations must not import mutable application code.
    conn = op.get_bind()
    rows = (
        conn.execute(
            text("""
      SELECT c.*,t.name||chr(58)||'v'||t.version AS tool_version FROM tool_calls c
      JOIN tool_versions t ON t.id=c.tool_version_id WHERE c.status='PENDING'
    """)
        )
        .mappings()
        .all()
    )
    for row in rows:
        payload = {"tool_version": row["tool_version"], "arguments": row["arguments"]}
        digest = hashlib.sha256(
            json.dumps(
                payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
            ).encode()
        ).hexdigest()
        conn.execute(
            text("""
          INSERT INTO tool_effects(id,tenant_id,project_id,run_id,step_id,tool_version_id,
            tool_version,idempotency_key,request_hash,arguments,status)
          VALUES(:step_id,:tenant_id,:project_id,:run_id,:step_id,:tool_version_id,
            :tool_version,:step_id,:digest,CAST(:arguments AS jsonb),'PREPARED')
        """),
            {**dict(row), "digest": digest, "arguments": json.dumps(row["arguments"])},
        )


def downgrade() -> None:
    op.execute("""
    LOCK TABLE public.work_items IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE public.tool_effects IN ACCESS EXCLUSIVE MODE;
    DO $$ BEGIN
      IF EXISTS(SELECT 1 FROM public.work_items WHERE status IN ('READY','PROCESSING'))
        OR EXISTS(SELECT 1 FROM public.tool_effects WHERE status IN
          ('PREPARED','DISPATCHED','OUTCOME_UNKNOWN')) THEN
        RAISE EXCEPTION 'Drain PROCESSING/READY work and resolve effects before downgrading';
      END IF;
    END $$;
    DROP TABLE tool_effects,mock_provider_results;
    ALTER TABLE run_attempts DROP CONSTRAINT attempt_effect_scope;
    ALTER TABLE runs DROP COLUMN cancel_epoch,DROP COLUMN cancellation_outcome;
    """)

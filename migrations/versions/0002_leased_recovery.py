"""Leases, physical attempts, metadata checkpoints and bounded mock recovery."""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    LOCK TABLE public.work_items IN ACCESS EXCLUSIVE MODE;
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM public.work_items WHERE status='PROCESSING') THEN
        RAISE EXCEPTION 'Drain Phase 1 PROCESSING work before upgrading; no automatic replay';
      END IF;
    END $$;
    DROP FUNCTION public.claim_runtime_work();
    ALTER TABLE work_items
      ADD COLUMN worker_id varchar(200),
      ADD COLUMN lease_token bigint NOT NULL DEFAULT 0 CHECK(lease_token>=0),
      ADD COLUMN lease_expires_at timestamptz,
      ADD COLUMN attempt_count integer NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
      ADD CONSTRAINT work_scope_id UNIQUE(tenant_id,project_id,run_id,step_id,id),
      ADD CONSTRAINT processing_has_lease CHECK(status<>'PROCESSING' OR
        (worker_id IS NOT NULL AND lease_expires_at IS NOT NULL AND lease_token>0));
    CREATE INDEX ix_work_expired ON work_items(lease_expires_at,id) WHERE status='PROCESSING';
    CREATE TABLE run_attempts (
      id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
      run_id varchar(64) NOT NULL,step_id varchar(64) NOT NULL,work_id varchar(64) NOT NULL,
      attempt_no integer NOT NULL CHECK(attempt_no>0),worker_id varchar(200) NOT NULL,
      lease_token bigint NOT NULL CHECK(lease_token>0),status varchar(32) NOT NULL
        CHECK(status IN ('RUNNING','SUCCEEDED','FAILED','ABANDONED',
                         'RETRY_SCHEDULED','OUTCOME_UNKNOWN')),
      started_at timestamptz NOT NULL DEFAULT clock_timestamp(),finished_at timestamptz,
      error_code varchar(100),
      UNIQUE(tenant_id,project_id,run_id,step_id,work_id,id),
      UNIQUE(work_id,attempt_no),UNIQUE(work_id,lease_token),
      FOREIGN KEY(tenant_id,project_id,run_id,step_id,work_id)
        REFERENCES work_items(tenant_id,project_id,run_id,step_id,id)
    );
    CREATE UNIQUE INDEX ix_one_live_attempt ON run_attempts(work_id) WHERE status='RUNNING';
    CREATE TABLE checkpoints (
      id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
      run_id varchar(64) NOT NULL,step_id varchar(64) NOT NULL,work_id varchar(64) NOT NULL,
      attempt_id varchar(64) NOT NULL,agent_version_id varchar(64) NOT NULL,
      schema_version integer NOT NULL DEFAULT 1,step_kind varchar(32) NOT NULL,
      result_ref varchar(200) NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      UNIQUE(tenant_id,project_id,run_id,step_id),
      FOREIGN KEY(tenant_id,project_id,run_id,step_id,work_id,attempt_id)
        REFERENCES run_attempts(tenant_id,project_id,run_id,step_id,work_id,id),
      FOREIGN KEY(tenant_id,project_id,agent_version_id)
        REFERENCES agent_versions(tenant_id,project_id,id)
    );
    CREATE TABLE dead_letter_items (
      id varchar(64) PRIMARY KEY,tenant_id varchar(64) NOT NULL,project_id varchar(64) NOT NULL,
      run_id varchar(64) NOT NULL,step_id varchar(64) NOT NULL,work_id varchar(64) NOT NULL,
      attempt_id varchar(64) NOT NULL,reason_code varchar(100) NOT NULL,
      created_at timestamptz NOT NULL DEFAULT clock_timestamp(),UNIQUE(work_id),
      FOREIGN KEY(tenant_id,project_id,run_id,step_id,work_id,attempt_id)
        REFERENCES run_attempts(tenant_id,project_id,run_id,step_id,work_id,id)
    );
    GRANT SELECT,INSERT,UPDATE ON run_attempts TO agent_app;
    GRANT SELECT,INSERT ON checkpoints,dead_letter_items TO agent_app;
    DO $$ DECLARE relation text; BEGIN
      FOREACH relation IN ARRAY ARRAY['run_attempts','checkpoints','dead_letter_items'] LOOP
        EXECUTE format('ALTER TABLE public.%I ENABLE ROW LEVEL SECURITY',relation);
        EXECUTE format('ALTER TABLE public.%I FORCE ROW LEVEL SECURITY',relation);
        EXECUTE format('CREATE POLICY tenant_scope ON public.%I TO agent_app '
          'USING (tenant_id=current_setting(''app.tenant_id'',true)) '
          'WITH CHECK (tenant_id=current_setting(''app.tenant_id'',true))',relation);
      END LOOP;
    END $$;
    """)
    op.execute("""
    CREATE FUNCTION public.claim_runtime_work(owner_id text,lease_seconds double precision)
    RETURNS TABLE(work_id varchar,scope_tenant varchar)
    LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
    BEGIN
      IF owner_id IS NULL OR length(owner_id)=0 OR length(owner_id)>200
        OR lease_seconds IS NULL OR NOT (lease_seconds>0 AND lease_seconds<=3600) THEN
        RAISE EXCEPTION 'Invalid lease parameters';
      END IF;
      RETURN QUERY
      UPDATE public.work_items AS w SET status='PROCESSING',worker_id=owner_id,
        lease_token=w.lease_token+1,attempt_count=w.attempt_count+1,
        lease_expires_at=clock_timestamp()+lease_seconds*interval '1 second'
      WHERE w.id=(SELECT q.id FROM public.work_items q
        JOIN public.runs r ON r.id=q.run_id AND r.tenant_id=q.tenant_id
          AND r.project_id=q.project_id
        WHERE q.status='READY' AND q.available_at<=clock_timestamp() AND r.state='QUEUED'
        ORDER BY q.available_at,q.id FOR UPDATE OF q SKIP LOCKED LIMIT 1)
      RETURNING w.id,w.tenant_id;
    END $$;
    ALTER FUNCTION public.claim_runtime_work(text,double precision) OWNER TO agent_dispatcher;
    REVOKE ALL ON FUNCTION public.claim_runtime_work(text,double precision) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION public.claim_runtime_work(text,double precision) TO agent_app;
    CREATE FUNCTION public.expired_runtime_work(batch_limit integer)
    RETURNS TABLE(work_id varchar,scope_tenant varchar)
    LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
      SELECT w.id,w.tenant_id FROM public.work_items w
      WHERE w.status='PROCESSING' AND w.lease_expires_at<=clock_timestamp()
      ORDER BY w.lease_expires_at,w.id
      FOR UPDATE OF w SKIP LOCKED LIMIT greatest(0,least(batch_limit,1000))
    $$;
    ALTER FUNCTION public.expired_runtime_work(integer) OWNER TO agent_dispatcher;
    REVOKE ALL ON FUNCTION public.expired_runtime_work(integer) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION public.expired_runtime_work(integer) TO agent_app;
    """)


def downgrade() -> None:
    op.execute("""
    LOCK TABLE public.work_items IN ACCESS EXCLUSIVE MODE;
    DO $$ BEGIN
      IF EXISTS(SELECT 1 FROM public.work_items WHERE status='PROCESSING') THEN
        RAISE EXCEPTION 'Drain PROCESSING work before downgrading';
      END IF;
    END $$;
    DROP FUNCTION public.expired_runtime_work(integer);
    DROP FUNCTION public.claim_runtime_work(text,double precision);
    DROP TABLE checkpoints,dead_letter_items,run_attempts;
    DROP INDEX ix_work_expired;
    ALTER TABLE work_items DROP CONSTRAINT processing_has_lease,DROP CONSTRAINT work_scope_id,
      DROP COLUMN worker_id,DROP COLUMN lease_token,DROP COLUMN lease_expires_at,
      DROP COLUMN attempt_count;
    CREATE FUNCTION public.claim_runtime_work() RETURNS TABLE(work_id varchar,scope_tenant varchar)
    LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public,pg_temp AS $$
      UPDATE public.work_items AS w SET status='PROCESSING'
      WHERE w.id=(SELECT q.id FROM public.work_items q
        JOIN public.runs r ON r.id=q.run_id AND r.tenant_id=q.tenant_id
          AND r.project_id=q.project_id
        WHERE q.status='READY' AND q.available_at<=now() AND r.state='QUEUED'
        ORDER BY q.available_at,q.id FOR UPDATE OF q SKIP LOCKED LIMIT 1)
      RETURNING w.id,w.tenant_id
    $$;
    ALTER FUNCTION public.claim_runtime_work() OWNER TO agent_dispatcher;
    REVOKE ALL ON FUNCTION public.claim_runtime_work() FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION public.claim_runtime_work() TO agent_app;
    """)

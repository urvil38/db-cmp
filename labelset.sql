CREATE TABLE IF NOT EXISTS tenant_labelset
(
    tenant       text,
    labels       jsonb,
    cardinality  text,
    created_at   int,
    updated_at   int,
    UNIQUE (tenant, cardinality)
);

CREATE INDEX IF NOT EXISTS tenant_labelset_last_seen_ix
    ON tenant_labelset USING BTREE (tenant, updated_at DESC);


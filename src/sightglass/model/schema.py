SCHEMA_VERSION = 10

RESIDENCY_SCHEMA_STATEMENTS: tuple[str, ...] = (
    # Per-conversation retention axis, kept deliberately separate from
    # ``ReaderPolicy`` access. Retention settings grant no read permission. The
    # ``mode`` column is the canonical residency decision that every growth path and
    # foreground read consults.
    """CREATE TABLE IF NOT EXISTS conversation_residency (
        conversation_id TEXT PRIMARY KEY
            REFERENCES conversations(conversation_id) ON DELETE CASCADE,
        mode TEXT NOT NULL CHECK (mode IN ('keep', 'recent', 'on_demand')),
        keep_backfill INTEGER NOT NULL DEFAULT 0,
        recent_window_days INTEGER,
        recent_max_bytes INTEGER,
        requested_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        reason TEXT
    )""",
    """CREATE INDEX IF NOT EXISTS conversation_residency_mode
        ON conversation_residency(mode, updated_at)""",
    # One row: global defaults and caps for the retention axis.  Kept content-free;
    # no observed payload, label or URL ever lands here.
    """CREATE TABLE IF NOT EXISTS residency_settings (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        default_mode TEXT NOT NULL DEFAULT 'on_demand'
            CHECK (default_mode IN ('keep', 'recent', 'on_demand')),
        recent_window_days INTEGER NOT NULL DEFAULT 30,
        recent_max_bytes INTEGER,
        global_max_bytes INTEGER,
        lease_ttl_seconds INTEGER NOT NULL DEFAULT 86400,
        lease_max_bytes INTEGER,
        updated_at TEXT NOT NULL
    )""",
    # Bounded, expiring foreground read cache.  A row records the exact message window
    # that a bounded read returned so a repeat read inside the TTL can be served
    # locally.  It is *not* resident body coverage: it never satisfies background
    # completeness and never claims live confirmation.
    """CREATE TABLE IF NOT EXISTS read_lease (
        lease_id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL
            REFERENCES conversations(conversation_id) ON DELETE CASCADE,
        scope_key TEXT NOT NULL,
        projection_epoch TEXT NOT NULL,
        lower_message_id TEXT,
        upper_message_id TEXT,
        message_count INTEGER NOT NULL,
        byte_size INTEGER NOT NULL,
        payload_digest TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )""",
    """CREATE INDEX IF NOT EXISTS read_lease_scope
        ON read_lease(conversation_id, scope_key, expires_at)""",
    """CREATE INDEX IF NOT EXISTS read_lease_expiry
        ON read_lease(expires_at)""",
    # Exact message/version dependencies for an evictable cache lease, never
    # a permanent pin on the whole conversation.
    """CREATE TABLE IF NOT EXISTS read_lease_message (
        lease_id TEXT NOT NULL REFERENCES read_lease(lease_id) ON DELETE CASCADE,
        message_id TEXT NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
        observation_seq INTEGER NOT NULL,
        PRIMARY KEY(lease_id, message_id)
    )""",
    """CREATE INDEX IF NOT EXISTS read_lease_message_message
        ON read_lease_message(message_id)""",
    """CREATE TABLE IF NOT EXISTS message_body_residency (
        message_id TEXT PRIMARY KEY REFERENCES messages(message_id) ON DELETE CASCADE,
        conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
        owner TEXT NOT NULL CHECK(owner IN ('protected', 'keep', 'recent', 'on_demand')),
        byte_size INTEGER NOT NULL,
        admitted_at TEXT NOT NULL,
        expires_at TEXT
    )""",
    """CREATE INDEX IF NOT EXISTS message_body_expiry
        ON message_body_residency(expires_at, message_id)""",
    """CREATE INDEX IF NOT EXISTS message_body_scope
        ON message_body_residency(conversation_id, owner, admitted_at, message_id)""",
    """CREATE TABLE IF NOT EXISTS residency_totals (
        conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
        owner TEXT NOT NULL,
        byte_size INTEGER NOT NULL DEFAULT 0,
        message_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(conversation_id, owner)
    )""",
    """CREATE TRIGGER IF NOT EXISTS body_residency_insert AFTER INSERT ON message_body_residency
        BEGIN
            INSERT INTO residency_totals VALUES
                (new.conversation_id, new.owner, new.byte_size, (new.byte_size>0))
            ON CONFLICT(conversation_id, owner) DO UPDATE SET
                byte_size=byte_size+new.byte_size, message_count=message_count+(new.byte_size>0);
        END""",
    """CREATE TRIGGER IF NOT EXISTS body_residency_delete AFTER DELETE ON message_body_residency
        BEGIN
            UPDATE residency_totals SET byte_size=byte_size-old.byte_size,
                message_count=message_count-(old.byte_size>0)
                WHERE conversation_id=old.conversation_id AND owner=old.owner;
        END""",
    """CREATE TRIGGER IF NOT EXISTS body_residency_update AFTER UPDATE ON message_body_residency
        BEGIN
            UPDATE residency_totals SET byte_size=byte_size-old.byte_size,
                message_count=message_count-(old.byte_size>0)
                WHERE conversation_id=old.conversation_id AND owner=old.owner;
            INSERT INTO residency_totals VALUES
                (new.conversation_id, new.owner, new.byte_size, (new.byte_size>0))
            ON CONFLICT(conversation_id, owner) DO UPDATE SET
                byte_size=byte_size+new.byte_size, message_count=message_count+(new.byte_size>0);
        END""",
    """CREATE INDEX IF NOT EXISTS message_resident_timeline
        ON messages(conversation_id,sort_primary,sort_seq,sort_tie,source_message_id)
        WHERE body_available=1""",
    """CREATE TABLE IF NOT EXISTS body_release_jobs (
        message_id TEXT PRIMARY KEY REFERENCES messages(message_id) ON DELETE CASCADE,
        observation_seq INTEGER NOT NULL,
        after_seq INTEGER NOT NULL DEFAULT 0
    )""",
    """CREATE TABLE IF NOT EXISTS residency_state (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1), revision INTEGER NOT NULL DEFAULT 0
    )""",
    "INSERT OR IGNORE INTO residency_state VALUES (1, 0)",
)

READING_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS source_read_windows (
        window_id INTEGER PRIMARY KEY,
        conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
        projection_epoch TEXT NOT NULL,
        lower_primary TEXT NOT NULL,
        lower_seq INTEGER NOT NULL,
        lower_tie INTEGER NOT NULL,
        lower_message_id TEXT NOT NULL,
        upper_primary TEXT NOT NULL,
        upper_seq INTEGER NOT NULL,
        upper_tie INTEGER NOT NULL,
        upper_message_id TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""",
    """CREATE INDEX IF NOT EXISTS source_read_windows_conversation
        ON source_read_windows(conversation_id, projection_epoch,
            lower_primary,lower_seq,lower_tie,lower_message_id)""",
    """CREATE TABLE IF NOT EXISTS observation_maintenance_state (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        after_message_id TEXT,
        complete INTEGER NOT NULL DEFAULT 0,
        revision INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL
    )""",
)

READING_COVERAGE_COLUMNS: tuple[str, ...] = (
    "coverage_version INTEGER NOT NULL DEFAULT 0",
    "contiguous_floor_position TEXT",
    "history_complete INTEGER NOT NULL DEFAULT 0",
    "forward_complete INTEGER NOT NULL DEFAULT 0",
)

LINK_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS message_links (
        link_id TEXT PRIMARY KEY,
        message_id TEXT NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
        account_id TEXT NOT NULL REFERENCES accounts(account_id),
        conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
        sent_at_utc TEXT NOT NULL,
        sort_seq INTEGER NOT NULL,
        sort_tie INTEGER NOT NULL,
        source_path TEXT NOT NULL,
        ordinal INTEGER NOT NULL,
        raw_url TEXT NOT NULL,
        normalized_url TEXT NOT NULL,
        scheme TEXT NOT NULL,
        normalized_host TEXT NOT NULL,
        path TEXT NOT NULL,
        query_text TEXT NOT NULL,
        fragment_text TEXT NOT NULL,
        title TEXT,
        description TEXT,
        source_kind TEXT NOT NULL,
        extraction_version TEXT NOT NULL,
        source_observation_seq INTEGER NOT NULL,
        link_digest TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(message_id, source_path, ordinal)
    )
    """,
    """CREATE INDEX IF NOT EXISTS message_links_host
        ON message_links(normalized_host, message_id, source_path, ordinal)""",
    """CREATE INDEX IF NOT EXISTS message_links_timeline
        ON message_links(account_id, sent_at_utc DESC, sort_seq DESC,
                         sort_tie DESC, message_id DESC, link_id DESC)""",
    """CREATE TABLE IF NOT EXISTS message_link_projection (
        message_id TEXT PRIMARY KEY REFERENCES messages(message_id) ON DELETE CASCADE,
        source_observation_seq INTEGER NOT NULL,
        input_digest TEXT NOT NULL,
        extraction_version TEXT NOT NULL,
        complete INTEGER NOT NULL,
        link_count INTEGER NOT NULL,
        updated_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS derived_index_state (
        index_kind TEXT PRIMARY KEY,
        recipe TEXT NOT NULL,
        generation INTEGER NOT NULL DEFAULT 1,
        checkpoint_seq INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL DEFAULT 'building',
        updated_at TEXT NOT NULL
    )""",
    """CREATE VIRTUAL TABLE IF NOT EXISTS message_lexical USING fts5(
        text, tokenize='trigram case_sensitive 1', detail=none,
        content='', contentless_delete=1
    )""",
    """CREATE TABLE IF NOT EXISTS message_lexical_projection (
        message_id TEXT PRIMARY KEY REFERENCES messages(message_id) ON DELETE CASCADE,
        source_observation_seq INTEGER NOT NULL,
        input_digest TEXT NOT NULL,
        recipe TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""",
    """CREATE INDEX IF NOT EXISTS message_search_timeline
        ON messages(conversation_id, current_state, sort_primary, sort_seq, sort_tie, message_id)
    """,
    """CREATE INDEX IF NOT EXISTS message_derived_backfill
        ON messages(first_observation_seq, message_id)
        WHERE first_observation_seq IS NOT NULL AND current_observation_seq IS NOT NULL""",
)


RESOURCE_JOB_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS resource_jobs (
        job_id TEXT PRIMARY KEY,
        resource_id TEXT NOT NULL REFERENCES resources(resource_id),
        resource_revision TEXT NOT NULL,
        recipe_digest TEXT NOT NULL,
        recipe_json TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN
            ('pending', 'leased', 'running', 'ready', 'failed', 'blocked', 'cancelled')),
        attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
        owner_id TEXT,
        lease_expires_at TEXT,
        fencing_token INTEGER CHECK (fencing_token IS NULL OR fencing_token >= 0),
        error_code TEXT,
        completed_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS resource_jobs_one_active_recipe
        ON resource_jobs(resource_id, resource_revision, recipe_digest)
        WHERE state IN ('pending', 'leased', 'running')
    """,
    """
    CREATE INDEX IF NOT EXISTS resource_jobs_state_schedule
        ON resource_jobs(state, updated_at, created_at)
    """,
    """
    CREATE INDEX IF NOT EXISTS resources_discovery_timeline
        ON resources(message_id, source_ordinal, resource_id)
    """,
)


RESOURCE_DISCOVERY_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE INDEX IF NOT EXISTS message_resource_discovery_timeline
        ON messages(
            account_id, current_state,
            sent_at_utc DESC, sort_seq DESC, sort_tie DESC, message_id DESC
        )
        WHERE first_observation_seq IS NOT NULL
          AND current_observation_seq IS NOT NULL
    """,
)


VOICE_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS voice_jobs (
        job_id TEXT PRIMARY KEY,
        account_id TEXT NOT NULL REFERENCES accounts(account_id),
        account_binding_id TEXT,
        resource_id TEXT NOT NULL REFERENCES resources(resource_id),
        resource_revision TEXT NOT NULL,
        input_digest TEXT REFERENCES resource_objects(object_digest),
        recipe_digest TEXT NOT NULL,
        recipe_json TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN
            ('pending', 'leased', 'running', 'ready', 'failed', 'blocked', 'cancelled')),
        attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
        owner_id TEXT,
        lease_expires_at TEXT,
        fencing_token INTEGER CHECK (fencing_token IS NULL OR fencing_token >= 0),
        max_duration_ms INTEGER CHECK (max_duration_ms IS NULL OR max_duration_ms > 0),
        max_bytes INTEGER CHECK (max_bytes IS NULL OR max_bytes > 0),
        result_digest TEXT REFERENCES resource_objects(object_digest),
        error_code TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS voice_jobs_one_active_per_input
        ON voice_jobs(account_id, resource_id, resource_revision, recipe_digest)
        WHERE state IN ('pending', 'leased', 'running', 'blocked')
    """,
    """
    CREATE INDEX IF NOT EXISTS voice_jobs_state_schedule
        ON voice_jobs(state, updated_at, created_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS voice_batches (
        batch_id TEXT PRIMARY KEY,
        reader_id TEXT NOT NULL REFERENCES reader_profiles(reader_id),
        account_id TEXT NOT NULL REFERENCES accounts(account_id),
        account_binding_id TEXT,
        selection_digest TEXT NOT NULL,
        recipe_digest TEXT NOT NULL,
        voice_policy TEXT NOT NULL CHECK (voice_policy IN ('auto', 'cached', 'off')),
        state TEXT NOT NULL DEFAULT 'open' CHECK (state IN
            ('open', 'sealed', 'delivered', 'expired', 'cancelled')),
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS voice_batches_reader_schedule
        ON voice_batches(reader_id, state, expires_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS voice_batch_items (
        batch_id TEXT NOT NULL REFERENCES voice_batches(batch_id),
        ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
        message_id TEXT NOT NULL REFERENCES messages(message_id),
        resource_id TEXT NOT NULL REFERENCES resources(resource_id),
        resource_revision TEXT NOT NULL,
        job_id TEXT REFERENCES voice_jobs(job_id),
        admission_step INTEGER NOT NULL CHECK (admission_step >= 0),
        state TEXT NOT NULL CHECK (state IN
            ('queued', 'admitted', 'rejected', 'cached', 'served', 'skipped')),
        PRIMARY KEY(batch_id, ordinal)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS voice_batch_items_job
        ON voice_batch_items(job_id) WHERE job_id IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS voice_batch_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL REFERENCES voice_batches(batch_id),
        item_ordinal INTEGER,
        job_id TEXT REFERENCES voice_jobs(job_id),
        kind TEXT NOT NULL CHECK (kind IN ('ready', 'failed', 'state-change')),
        result_digest TEXT REFERENCES resource_objects(object_digest),
        created_at TEXT NOT NULL,
        FOREIGN KEY (batch_id, item_ordinal) REFERENCES voice_batch_items(batch_id, ordinal)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS voice_batch_events_batch
        ON voice_batch_events(batch_id, event_id)
    """,
)


# The two `resources` partial unique indexes cannot serve a bare ``WHERE message_id = ?``
# lookup, because nothing in that predicate implies their partial conditions.  Every
# per-message ingest therefore scanned the whole table through the primary-key index.
RESOURCE_INDEX_STATEMENTS: tuple[str, ...] = (
    """
    CREATE INDEX IF NOT EXISTS resources_message_id
        ON resources(message_id)
    """,
)


SCHEMA_SQL = (
    """
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    source_namespace TEXT NOT NULL UNIQUE,
    source_account_key TEXT,
    identity_confidence TEXT NOT NULL,
    reader_timezone TEXT NOT NULL,
    current_display_name TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    account_binding_id TEXT,
    source_inventory_epoch TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS accounts_source_account_key_unique
    ON accounts(source_account_key) WHERE source_account_key IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS accounts_binding_id_unique
    ON accounts(account_binding_id) WHERE account_binding_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS conversations (
    conversation_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    source_conversation_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    current_title TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_message_at TEXT,
    visibility_state TEXT NOT NULL DEFAULT 'active',
    roster_complete INTEGER NOT NULL DEFAULT 0,
    catalog_state TEXT NOT NULL DEFAULT 'unknown',
    unread_count INTEGER,
    catalog_observed_at TEXT,
    UNIQUE(account_id, source_conversation_id)
);
CREATE INDEX IF NOT EXISTS conversation_catalog
    ON conversations(account_id, catalog_state, last_message_at, conversation_id);

CREATE TABLE IF NOT EXISTS conversation_aliases (
    conversation_alias_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    alias TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    alias_kind TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    source TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS conversation_alias_lookup
    ON conversation_aliases(conversation_id, normalized_alias, active);

CREATE TABLE IF NOT EXISTS participants (
    participant_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    current_reader_label TEXT,
    is_self INTEGER NOT NULL DEFAULT 0,
    actor_kind TEXT NOT NULL,
    resolution_state TEXT NOT NULL,
    identity_confidence TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS participants_one_self_per_account
    ON participants(account_id) WHERE is_self = 1;

CREATE TABLE IF NOT EXISTS participant_source_keys (
    source_key_id TEXT PRIMARY KEY,
    participant_id TEXT NOT NULL REFERENCES participants(participant_id),
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    key_kind TEXT NOT NULL,
    key_value TEXT NOT NULL,
    scope_conversation_id TEXT REFERENCES conversations(conversation_id),
    stability TEXT NOT NULL,
    principal_eligible INTEGER NOT NULL,
    provenance TEXT NOT NULL,
    first_observed_at TEXT NOT NULL,
    last_observed_at TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE UNIQUE INDEX IF NOT EXISTS participant_source_keys_global_unique
    ON participant_source_keys(account_id, key_kind, key_value)
    WHERE scope_conversation_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS participant_source_keys_scoped_unique
    ON participant_source_keys(account_id, key_kind, key_value, scope_conversation_id)
    WHERE scope_conversation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS participant_labels (
    label_id TEXT PRIMARY KEY,
    participant_id TEXT NOT NULL REFERENCES participants(participant_id),
    label TEXT NOT NULL,
    normalized_label TEXT NOT NULL,
    label_kind TEXT NOT NULL,
    scope_kind TEXT NOT NULL,
    reader_id TEXT,
    observed_message_id TEXT,
    provenance TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    temporal_confidence TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS participant_label_lookup
    ON participant_labels(participant_id, normalized_label, active);

CREATE TABLE IF NOT EXISTS identity_corrections (
    correction_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    subject_json TEXT NOT NULL,
    reason TEXT,
    created_at TEXT NOT NULL,
    operator_identity TEXT NOT NULL,
    supersedes_correction_id TEXT REFERENCES identity_corrections(correction_id)
);

CREATE TABLE IF NOT EXISTS conversation_members (
    membership_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    participant_id TEXT NOT NULL REFERENCES participants(participant_id),
    source_membership_id TEXT,
    current_group_alias TEXT,
    resolution_state TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_message_at TEXT,
    UNIQUE(conversation_id, participant_id)
);

CREATE TABLE IF NOT EXISTS conversation_member_labels (
    member_label_id TEXT PRIMARY KEY,
    membership_id TEXT NOT NULL REFERENCES conversation_members(membership_id),
    label TEXT NOT NULL,
    normalized_label TEXT NOT NULL,
    label_kind TEXT NOT NULL,
    observed_message_id TEXT,
    provenance TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT,
    temporal_confidence TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS member_label_lookup
    ON conversation_member_labels(membership_id, normalized_label, active);

CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    source_message_id TEXT NOT NULL,
    source_time_raw TEXT NOT NULL,
    sent_at_utc TEXT NOT NULL,
    sort_primary TEXT NOT NULL,
    sort_seq INTEGER NOT NULL,
    sort_tie INTEGER NOT NULL,
    sender_id TEXT REFERENCES participants(participant_id),
    sender_membership_id TEXT REFERENCES conversation_members(membership_id),
    sender_label_snapshot_json TEXT NOT NULL,
    kind TEXT NOT NULL,
    text TEXT,
    structured_json TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    current_state TEXT NOT NULL,
    current_generation_id TEXT NOT NULL,
    search_text TEXT,
    projection_epoch TEXT,
    first_observation_seq INTEGER,
    current_observation_seq INTEGER,
    body_available INTEGER NOT NULL DEFAULT 1,
    UNIQUE(account_id, source_message_id)
);
CREATE INDEX IF NOT EXISTS message_timeline
    ON messages(conversation_id, sort_primary, sort_seq, sort_tie, source_message_id);
CREATE INDEX IF NOT EXISTS message_sender_timeline
    ON messages(conversation_id, sender_id, sort_primary, sort_seq, sort_tie, source_message_id);
CREATE INDEX IF NOT EXISTS message_materialized_timeline
    ON messages(
        conversation_id, projection_epoch, current_state,
        sort_primary, sort_seq, sort_tie, source_message_id
    );

CREATE TABLE IF NOT EXISTS message_observations (
    observation_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id TEXT NOT NULL UNIQUE,
    message_id TEXT NOT NULL REFERENCES messages(message_id),
    observed_at TEXT NOT NULL,
    source_generation_id TEXT NOT NULL,
    state TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    parsed_json TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    raw_payload_ref TEXT,
    reason_code TEXT
);
CREATE INDEX IF NOT EXISTS message_observation_projection_identity
    ON message_observations(message_id, state, payload_digest, parser_version);

CREATE TABLE IF NOT EXISTS resources (
    resource_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL REFERENCES messages(message_id),
    source_resource_key TEXT,
    source_ordinal INTEGER NOT NULL,
    kind TEXT NOT NULL,
    mime_type TEXT,
    original_name TEXT,
    declared_size INTEGER,
    declared_hash TEXT,
    availability TEXT NOT NULL,
    resolver_json TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS resources_source_key_unique
    ON resources(message_id, source_resource_key)
    WHERE source_resource_key IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS resources_ordinal_unique
    ON resources(message_id, source_ordinal)
    WHERE source_resource_key IS NULL;

CREATE TABLE IF NOT EXISTS resource_objects (
    object_digest TEXT PRIMARY KEY,
    local_path_internal TEXT NOT NULL,
    mime_type TEXT,
    byte_size INTEGER NOT NULL,
    origin TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_verified_at TEXT
);

CREATE TABLE IF NOT EXISTS resource_bindings (
    resource_id TEXT NOT NULL REFERENCES resources(resource_id),
    object_digest TEXT NOT NULL REFERENCES resource_objects(object_digest),
    variant TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(resource_id, variant)
);

CREATE TABLE IF NOT EXISTS resource_derivations (
    derivation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(resource_id),
    source_digest TEXT NOT NULL,
    variant TEXT NOT NULL,
    processor_name TEXT NOT NULL,
    processor_version TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    derived_digest TEXT NOT NULL REFERENCES resource_objects(object_digest),
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS resource_derivations_provenance_unique
    ON resource_derivations(
        resource_id,
        source_digest,
        variant,
        processor_name,
        processor_version,
        parameters_json,
        derived_digest
    );
CREATE INDEX IF NOT EXISTS resource_derivations_derived_object
    ON resource_derivations(derived_digest);

CREATE TABLE IF NOT EXISTS reader_profiles (
    reader_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    auth_token_hash TEXT,
    policy_json TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    policy_revision INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS reader_timeline_cursors (
    reader_id TEXT NOT NULL REFERENCES reader_profiles(reader_id),
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    scope_kind TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    committed_sort_primary TEXT NOT NULL,
    committed_sort_tie TEXT NOT NULL,
    committed_message_id TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(reader_id, conversation_id, scope_kind, scope_key)
);

CREATE TABLE IF NOT EXISTS source_scan_cursors (
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    source_shard_key TEXT NOT NULL,
    source_generation_id TEXT NOT NULL,
    cursor_token TEXT NOT NULL,
    overlap_policy_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(account_id, conversation_id, source_shard_key)
);

CREATE TABLE IF NOT EXISTS source_catalog_state (
    account_id TEXT PRIMARY KEY REFERENCES accounts(account_id),
    source_inventory_epoch TEXT,
    coverage_state TEXT NOT NULL DEFAULT 'unknown',
    next_cursor_token TEXT,
    scan_started_at TEXT,
    scan_completed_at TEXT,
    last_observed_at TEXT,
    last_error_code TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_conversation_state (
    conversation_id TEXT PRIMARY KEY REFERENCES conversations(conversation_id),
    source_inventory_epoch TEXT,
    tail_cursor_token TEXT,
    tail_generation_id TEXT,
    tail_sort_primary TEXT,
    tail_sort_seq INTEGER,
    tail_sort_tie INTEGER,
    tail_source_message_id TEXT,
    tail_observed_at TEXT,
    indexed_before TEXT,
    indexed_after TEXT,
    backfill_state TEXT NOT NULL DEFAULT 'not_started',
    last_error_code TEXT,
    updated_at TEXT NOT NULL,
    coverage_version INTEGER NOT NULL DEFAULT 0,
    contiguous_floor_position TEXT,
    history_complete INTEGER NOT NULL DEFAULT 0,
    forward_complete INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS source_conversation_backfill_state
    ON source_conversation_state(backfill_state, updated_at);

CREATE TABLE IF NOT EXISTS source_shard_state (
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    source_shard_key TEXT NOT NULL,
    source_inventory_epoch TEXT,
    source_generation_id TEXT,
    availability_state TEXT NOT NULL DEFAULT 'unknown',
    cursor_token TEXT,
    last_sort_primary TEXT,
    last_sort_seq INTEGER,
    last_sort_tie INTEGER,
    last_source_message_id TEXT,
    discovered_at TEXT NOT NULL,
    last_verified_at TEXT,
    last_error_code TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(account_id, source_shard_key)
);
CREATE INDEX IF NOT EXISTS source_shard_availability
    ON source_shard_state(account_id, availability_state, updated_at);

CREATE TABLE IF NOT EXISTS source_backfill_jobs (
    job_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(account_id),
    conversation_id TEXT REFERENCES conversations(conversation_id),
    source_inventory_epoch TEXT,
    requested_after TEXT,
    requested_before TEXT,
    max_messages INTEGER NOT NULL,
    processed_messages INTEGER NOT NULL DEFAULT 0,
    cursor_token TEXT,
    state TEXT NOT NULL DEFAULT 'queued',
    created_at TEXT NOT NULL,
    started_at TEXT,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    last_error_code TEXT
);
CREATE INDEX IF NOT EXISTS source_backfill_jobs_schedule
    ON source_backfill_jobs(state, updated_at, created_at);

CREATE TABLE IF NOT EXISTS reader_update_cursors (
    reader_id TEXT NOT NULL REFERENCES reader_profiles(reader_id),
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    scope_kind TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    committed_observation_seq INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(reader_id, conversation_id, scope_kind, scope_key)
);

CREATE TABLE IF NOT EXISTS reader_deliveries (
    delivery_id TEXT PRIMARY KEY,
    reader_id TEXT NOT NULL REFERENCES reader_profiles(reader_id),
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    scope_kind TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    from_observation_seq INTEGER NOT NULL,
    to_observation_seq INTEGER NOT NULL,
    projection_schema_version TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_ref TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    acknowledged_at TEXT,
    expires_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS reader_deliveries_one_pending
    ON reader_deliveries(reader_id, conversation_id, scope_kind, scope_key)
    WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS access_receipts (
    receipt_id TEXT PRIMARY KEY,
    reader_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    conversation_id TEXT,
    scope_kind TEXT,
    scope_digest TEXT,
    message_count INTEGER NOT NULL,
    resource_count INTEGER NOT NULL,
    bytes_returned INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    outcome TEXT NOT NULL,
    warning_codes_json TEXT NOT NULL
);
"""
    + "".join(f"{statement};\n" for statement in VOICE_SCHEMA_STATEMENTS)
    + "".join(f"{statement};\n" for statement in RESOURCE_INDEX_STATEMENTS)
    + "".join(f"{statement};\n" for statement in RESOURCE_JOB_SCHEMA_STATEMENTS)
    + "".join(f"{statement};\n" for statement in RESOURCE_DISCOVERY_SCHEMA_STATEMENTS)
    + "".join(f"{statement};\n" for statement in LINK_SCHEMA_STATEMENTS)
    + "".join(f"{statement};\n" for statement in READING_SCHEMA_STATEMENTS)
    + "".join(f"{statement};\n" for statement in RESIDENCY_SCHEMA_STATEMENTS)
)

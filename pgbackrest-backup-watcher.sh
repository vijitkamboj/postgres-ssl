#!/bin/bash
# pgbackrest-backup-watcher.sh — long-running daemon that triggers pgBackRest
# base backups based on archiving health. Forked from wrapper.sh at container
# start when WAL_ARCHIVE_BUCKET is set; same pattern as bootstrap_pgbackrest_stanza.
#
# Triggers (any of):
#   1. NEEDS_INITIAL_BACKUP — first archive-push success after enable. Takes
#      the first full so PITR is restorable from this LSN forward. Replaces
#      v1's "immediate snapshot on enable" race: pgbackrest backup brackets
#      the base in pg_backup_start/stop so the LSN window of the base and
#      the WAL covering it are the same thing — no coordination gap.
#   2. Gap recovery — archive-push had hard failures since the last full
#      (any of: pgbackrest-archive-push-wrapper.sh dropped a segment and
#      touched .pgbackrest_gap_pending, pg_stat_archiver.failed_count grew
#      since the last full's checkpoint, or the LSN-lag probe found
#      pg_stat_archiver.last_archived_wal more than
#      WAL_LAG_GAP_THRESHOLD_SEGMENTS ahead of the S3 catalog's high-water).
#      Once failures are decisively over, runs a fresh full so PITR window
#      resumes from this base forward.
#   3. Periodic — full every WAL_BACKUP_FULL_INTERVAL_HOURS, diff every
#      WAL_BACKUP_DIFF_INTERVAL_HOURS.
#
# State persists at $PGDATA/.pgbackrest_backup_state (key=value lines, no JSON
# dep). The bucket-side `pgbackrest --stanza=main info` is the canonical
# source of truth for backup history; the local file is a cache that survives
# restarts. A wiped volume / fresh failover-promote with stale local state
# triggers an extra full — harmless, pgBackRest's stanza locks prevent
# concurrent backups across nodes.
#
# HA: every node runs the watcher. Standbys exit early via pg_is_in_recovery().
# Only the leader runs backups. v1 of this watcher backs up from the primary;
# `--backup-standby` is a follow-up.
#
# Idle-DB heartbeat: each iteration emits a tiny non-transactional WAL record
# via pg_logical_emit_message. Without it, idle Postgres never advances the
# LSN, so archive_timeout=60 never forces a segment switch and
# pg_stat_archiver.last_archived_time stalls until the next CHECKPOINT
# (default 5 min) — meaning the picker's "latest restorable" lags wall-clock
# by minutes on quiet services. The heartbeat keeps PITR RPO tracking
# archive_timeout (~60s) instead of checkpoint_timeout (~5min). Cost is
# ~one 16MB WAL segment per minute on idle DBs (zstd-3 compresses to a
# handful of KB → ~30-70MB/day). Set WAL_HEARTBEAT_DISABLED=1 to skip.
#
# LSN-lag detection: pgBackRest async mode returns archive_command success to
# Postgres as soon as the WAL segment lands in the local spool, BEFORE the
# async worker uploads it to S3. If the async worker hangs, dies without
# releasing its lock, or hits an unrecoverable upload error, the spool keeps
# accepting WAL (foreground returns 0) while the S3 catalog stays frozen.
# archive-push-queue-max eventually drops segments and ALSO returns 0 to
# Postgres — so pg_stat_archiver.failed_count never increments and the
# archive-push wrapper never sees a non-zero exit. Both other gap signals
# (failed_count growth, wrapper-touched marker) miss this entirely. The
# LSN-lag probe re-uses the same comparison the admin monitor performs on
# the backboard side: `pgbackrest info`'s archive max segment per timeline
# against pg_stat_archiver.last_archived_wal. Threshold matches the
# monitor's WAL_ARCHIVE_LAG_CRIT_SEGMENTS (64 segments ≈ 1 GiB) so the
# in-image self-heal fires at the same point the dashboard surfaces the
# warning.

set -u

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
STATE_FILE="$PGDATA/.pgbackrest_backup_state"
GAP_MARKER="$PGDATA/.pgbackrest_gap_pending"

# POLL_INTERVAL_SECONDS / GAP_RESOLVED_GRACE_SECONDS are env-overridable so
# the e2e harness can exercise gap-recovery in <1 min instead of 5+. The
# defaults are conservative; nothing user-facing advertises these knobs.
POLL_INTERVAL_SECONDS="${WAL_BACKUP_POLL_INTERVAL_SECONDS:-60}"

# Until the first full lands the loop polls on a tighter cadence so a race
# with wrapper.sh's bootstrap stanza-create (or a slow first postmaster
# bind) doesn't cost a full minute per retry. After that, normal cadence.
INITIAL_POLL_SECONDS="${WAL_BACKUP_INITIAL_POLL_SECONDS:-5}"

# Failures must have been quiescent for this long before a gap-recovery backup
# fires. Hard failures often resolve and re-fail (intermittent S3, half-rotated
# creds); without the grace the watcher burns one full per flap.
GAP_RESOLVED_GRACE_SECONDS="${WAL_BACKUP_GAP_RESOLVED_GRACE_SECONDS:-300}"

FULL_INTERVAL_HOURS="${WAL_BACKUP_FULL_INTERVAL_HOURS:-168}"
DIFF_INTERVAL_HOURS="${WAL_BACKUP_DIFF_INTERVAL_HOURS:-24}"

# How often to verify the S3 catalog actually contains a full backup (seconds).
# Catches divergence between local state (last_full_at) and S3 reality — e.g.
# the backup command returned exit 0 but the catalog write never completed, or a
# volume survived a redeployment with stale state pointing at a different stanza
# path. 0 disables periodic verification (NEEDS_INITIAL_BACKUP still fires on
# fresh state). Default: 3600 (1 hour).
CATALOG_VERIFY_INTERVAL_SECONDS="${WAL_BACKUP_CATALOG_VERIFY_INTERVAL_SECONDS:-3600}"

# LSN-lag detection — see file header. Threshold matches the admin monitor's
# WAL_ARCHIVE_LAG_CRIT_SEGMENTS. Probe cadence is the dominant cost (one
# `pgbackrest info` S3 round-trip per probe; ~50-200ms typical), so don't run
# it every iteration. Default 300s (5min) gives ~5 min worst-case detection
# latency, fast enough that the gap-recovery grace then the resulting full
# happen well within an hour of the underlying break.
WAL_LAG_GAP_THRESHOLD_SEGMENTS="${WAL_LAG_GAP_THRESHOLD_SEGMENTS:-64}"
WAL_LAG_PROBE_INTERVAL_SECONDS="${WAL_LAG_PROBE_INTERVAL_SECONDS:-300}"

# Resolved cadence in seconds. WAL_BACKUP_FULL_INTERVAL_SECONDS overrides
# the hours setting — bash arithmetic precludes fractional hours, so the
# e2e harness needs a second-level knob to exercise retention rollover
# inside a single test cycle. 0 means "no periodic full" (gap-recovery
# and NEEDS_INITIAL_BACKUP still fire); any positive value sets the
# cadence. Defaults to FULL_INTERVAL_HOURS * 3600 when unset, preserving
# existing prod behavior.
FULL_INTERVAL_SECONDS="${WAL_BACKUP_FULL_INTERVAL_SECONDS:-$((FULL_INTERVAL_HOURS * 3600))}"
DIFF_INTERVAL_SECONDS="${WAL_BACKUP_DIFF_INTERVAL_SECONDS:-$((DIFF_INTERVAL_HOURS * 3600))}"

log() { echo "pgbackrest-watcher: $*"; }

# State file is `key=value\n`-shaped: trivially read/written by bash without
# adding a JSON dep. Schema (all values are integer epoch seconds or counts):
#   last_full_at=<epoch>             — last successful full backup
#   last_diff_at=<epoch>             — last successful diff/incr backup
#   last_full_failed_count=<int>     — pg_stat_archiver.failed_count after last full
#   last_catalog_verify_at=<epoch>   — last S3 catalog probe (catalog_check_backup)
#   last_lag_detected_at=<epoch>     — when LSN-lag first crossed threshold this cycle
read_state() {
  local field="$1"
  [ ! -f "$STATE_FILE" ] && return 0
  grep -E "^${field}=" "$STATE_FILE" 2>/dev/null | tail -1 | cut -d= -f2-
}

write_state_field() {
  local field="$1" value="$2"
  local tmp
  tmp=$(mktemp "$STATE_FILE.XXXX") || return 1
  if [ -f "$STATE_FILE" ]; then
    grep -vE "^${field}=" "$STATE_FILE" > "$tmp" 2>/dev/null || true
  fi
  echo "${field}=${value}" >> "$tmp"
  mv "$tmp" "$STATE_FILE"
}

# Stats from pg_stat_archiver. Sets globals so callers can branch on them
# without repeated psql round-trips.
ARCHIVED_COUNT=0
FAILED_COUNT=0
LAST_ARCHIVED_EPOCH=0
LAST_FAILED_EPOCH=0
LAST_ARCHIVED_WAL=""

# COALESCE(last_archived_wal, '-') keeps the field non-empty so `read -r`'s
# whitespace IFS-splitting doesn't collapse a trailing empty column into the
# previous one and corrupt the bind. The sentinel is stripped below.
refresh_archiver_stats() {
  local stats wal_field
  stats=$(psql -U postgres -tAXq -F' ' -c "
    SELECT
      archived_count,
      failed_count,
      COALESCE(EXTRACT(EPOCH FROM last_archived_time)::bigint, 0),
      COALESCE(EXTRACT(EPOCH FROM last_failed_time)::bigint, 0),
      COALESCE(last_archived_wal, '-')
    FROM pg_stat_archiver
  " 2>/dev/null) || return 1
  [ -z "$stats" ] && return 1
  read -r ARCHIVED_COUNT FAILED_COUNT LAST_ARCHIVED_EPOCH LAST_FAILED_EPOCH wal_field <<<"$stats"
  if [ "$wal_field" = "-" ]; then
    LAST_ARCHIVED_WAL=""
  else
    LAST_ARCHIVED_WAL="$wal_field"
  fi
}

# 0 = standby (skip backups). 1 = leader-or-unknown (proceed; pgBackRest's
# stanza lock is the second-line guarantee against double-trigger).
is_standby() {
  local r
  r=$(psql -U postgres -tAXq -c "SELECT pg_is_in_recovery()" 2>/dev/null) || return 1
  [ "$r" = "t" ]
}

# Returns 0 if archive failures look decisively over. Considers both the
# pg_stat_archiver.last_failed_time epoch AND the LSN-lag detection epoch
# (last_lag_detected_at). Either signal still within grace blocks recovery;
# both signals quiescent (or never tripped) clears for a fresh full.
gap_recovered() {
  local now="$1" last_fail="$2"
  local last_lag
  last_lag=$(read_state last_lag_detected_at)
  : "${last_lag:=0}"
  if [ "$last_fail" -gt 0 ] && [ $((now - last_fail)) -lt "$GAP_RESOLVED_GRACE_SECONDS" ]; then
    return 1
  fi
  if [ "$last_lag" -gt 0 ] && [ $((now - last_lag)) -lt "$GAP_RESOLVED_GRACE_SECONDS" ]; then
    return 1
  fi
  return 0
}

run_backup() {
  local type="$1"
  log "running pgbackrest backup --type=$type"
  # --repo=1 scopes backup + post-backup expire to this service's own bucket.
  # On a fork repo2 is source's read-only bucket; without the pin pgBackRest
  # would default to writing the new base into both repos.
  pgbackrest --stanza=main --repo=1 backup --type="$type"
  local exit_code=$?

  # Exit 55 = FileMissingError: backup.info absent — stanza was never
  # initialized (bootstrap stanza-create failed or timed out on first boot).
  # Run stanza-create now and retry once; the watcher loop handles the rest.
  if [ "$exit_code" -eq 55 ]; then
    log "stanza not initialized (exit 55), running stanza-create then retrying backup..."
    pgbackrest --stanza=main stanza-create || true
    pgbackrest --stanza=main --repo=1 backup --type="$type"
    exit_code=$?
  fi

  if [ "$exit_code" -ne 0 ]; then
    log "backup --type=$type failed (will retry on next poll)"
    return 1
  fi

  local now; now=$(date +%s)
  case "$type" in
    full)
      write_state_field last_full_at "$now"
      write_state_field last_diff_at "$now"
      # Re-read failed_count *after* the backup so a failure during the
      # backup itself is folded into the high-water mark; otherwise the
      # next iteration would see growth and re-trigger immediately.
      refresh_archiver_stats || true
      write_state_field last_full_failed_count "$FAILED_COUNT"
      # last_lag_detected_at is gap-recovery state same as last_failed_count;
      # clearing alongside the gap marker keeps gap_recovered's next-round
      # grace window honest. Without this, a fresh detection right after a
      # successful full would still see the stale epoch.
      write_state_field last_lag_detected_at 0
      [ -f "$GAP_MARKER" ] && rm -f "$GAP_MARKER" && log "cleared gap marker"
      ;;
    diff|incr)
      write_state_field last_diff_at "$now"
      ;;
  esac
  log "backup --type=$type completed"
  emit_pitr_anchor
  return 0
}

# Probes the S3 catalog for repo1 via pgbackrest info --output=json.
# No text-parsing heuristics needed. Returns three distinct states:
#   0 — full backup confirmed present (pgbackrest exit 0 + "full" in output)
#   1 — conclusively no full backup (pgbackrest exit 0, no "full" entry)
#   2 — inconclusive (pgbackrest exited non-zero: S3 unreachable, auth failure,
#       stanza not yet created, etc.) — caller must NOT clear local state
catalog_check_backup() {
  local info_out rc
  info_out=$(timeout 60 pgbackrest --stanza=main --repo=1 info --output=json 2>/dev/null)
  rc=$?
  [ "$rc" -ne 0 ] && return 2
  printf '%s' "$info_out" | grep -q '"type":"full"' && return 0
  return 1
}

# LSN-lag probe state. LAST_LAG_PROBE_AT throttles probe_archive_lag to
# WAL_LAG_PROBE_INTERVAL_SECONDS; LAST_OBSERVED_LAG_SEGMENTS surfaces the
# most recent observation for the watcher_iteration diagnostic log.
LAST_LAG_PROBE_AT=0
LAST_OBSERVED_LAG_SEGMENTS=0
LAST_LAG_REPO_MAX=""

# 24-char hex WAL filename → absolute segment count (256 segments per log
# file under the default 16 MiB wal_segment_size). Echoes empty on malformed
# input so callers short-circuit. Strict shape check before the arithmetic
# avoids letting a stray non-hex character feed `$((16#…))` and crash the
# watcher via set -u + arithmetic failure.
segment_to_number() {
  local wal="$1"
  [ ${#wal} -eq 24 ] || return 0
  case "$wal" in
    *[!0-9A-Fa-f]*) return 0 ;;
  esac
  local log seg
  log=$((16#${wal:8:8}))
  seg=$((16#${wal:16:8}))
  echo $((log * 256 + seg))
}

# Run `pgbackrest info` and extract the highest archived WAL segment on the
# same timeline as LAST_ARCHIVED_WAL, then compute lag against
# pg_stat_archiver's handoff high-water. Updates LAST_OBSERVED_LAG_SEGMENTS
# and LAST_LAG_REPO_MAX. Returns 0 on a successful probe (even when lag is
# 0); returns 1 on transient failure so callers leave prior state in place
# rather than acting on noise.
#
# The JSON is text-parsed because jq isn't in the base image and `pgbackrest
# info`'s archive section has a stable schema: each archive entry has a
# "max":"<24-hex>" key per timeline. Filtering by the leading 8 hex chars
# (timeline ID) picks the right entry without a full parser.
probe_archive_lag() {
  [ -z "$LAST_ARCHIVED_WAL" ] && { LAST_OBSERVED_LAG_SEGMENTS=0; return 0; }
  local handed_off_n; handed_off_n=$(segment_to_number "$LAST_ARCHIVED_WAL")
  [ -z "$handed_off_n" ] && return 1

  local info_out
  info_out=$(timeout 30 pgbackrest --stanza=main --repo=1 info --output=json 2>/dev/null) || return 1
  [ -z "$info_out" ] && return 1

  local tl="${LAST_ARCHIVED_WAL:0:8}"
  local repo_max
  repo_max=$(printf '%s' "$info_out" \
    | grep -oE "\"max\":\"${tl}[0-9A-Fa-f]{16}\"" \
    | sort -u | tail -1 \
    | sed -E 's/.*"([0-9A-Fa-f]{24})".*/\1/')
  if [ -z "$repo_max" ]; then
    # Same timeline not in catalog yet (fresh stanza, no archived WAL).
    # Treat as zero lag — NEEDS_INITIAL_BACKUP / stanza-create paths cover
    # the empty-catalog cases without our help.
    LAST_OBSERVED_LAG_SEGMENTS=0
    LAST_LAG_REPO_MAX=""
    return 0
  fi

  local repo_max_n; repo_max_n=$(segment_to_number "$repo_max")
  [ -z "$repo_max_n" ] && return 1
  local lag=$((handed_off_n - repo_max_n))
  [ "$lag" -lt 0 ] && lag=0
  LAST_OBSERVED_LAG_SEGMENTS="$lag"
  LAST_LAG_REPO_MAX="$repo_max"
  return 0
}

# Throttled wrapper around probe_archive_lag that writes the gap marker +
# last_lag_detected_at state field when observed lag crosses the threshold.
# Idempotent: re-detecting an already-marked gap only refreshes the log
# breadcrumb, never re-stamps last_lag_detected_at (that would make
# gap_recovered's grace check sticky and prevent the gap from ever clearing).
check_lsn_lag_and_mark_gap() {
  local now; now=$(date +%s)
  if [ $((now - LAST_LAG_PROBE_AT)) -lt "$WAL_LAG_PROBE_INTERVAL_SECONDS" ]; then
    return 0
  fi
  LAST_LAG_PROBE_AT="$now"

  if ! probe_archive_lag; then
    log "lag probe inconclusive (pgbackrest info failed or malformed); leaving prior state"
    return 0
  fi

  if [ "$LAST_OBSERVED_LAG_SEGMENTS" -lt "$WAL_LAG_GAP_THRESHOLD_SEGMENTS" ]; then
    return 0
  fi

  if [ ! -f "$GAP_MARKER" ]; then
    touch "$GAP_MARKER"
    write_state_field last_lag_detected_at "$now"
    log "LSN-lag gap detected (handoff=${LAST_ARCHIVED_WAL}, repo_max=${LAST_LAG_REPO_MAX:-?}, lag=${LAST_OBSERVED_LAG_SEGMENTS} segments ≥ threshold ${WAL_LAG_GAP_THRESHOLD_SEGMENTS}) — marking gap_pending"
  else
    log "LSN-lag persists (handoff=${LAST_ARCHIVED_WAL}, repo_max=${LAST_LAG_REPO_MAX:-?}, lag=${LAST_OBSERVED_LAG_SEGMENTS} segments)"
  fi
}

# Sets DECIDED_ACTION to "full"|"diff"|"" (no action). Runs in the caller's
# shell — not a subshell — so the diagnostic globals (LAST_FULL_DIAG,
# GAP_MARKER_DIAG, LAST_FULL_FAILED_DIAG) survive for watcher_iteration to
# log. Without these, a misbehaving cluster looks indistinguishable from a
# correctly-idle one in production logs.
decide_action() {
  DECIDED_ACTION=""
  local now; now=$(date +%s)
  local last_full last_diff last_full_failed
  last_full=$(read_state last_full_at)
  last_diff=$(read_state last_diff_at)
  last_full_failed=$(read_state last_full_failed_count)
  : "${last_full_failed:=0}"
  LAST_FULL_DIAG="${last_full:-empty}"
  LAST_FULL_FAILED_DIAG="$last_full_failed"
  GAP_MARKER_DIAG=$([ -f "$GAP_MARKER" ] && echo "present" || echo "absent")

  # NEEDS_INITIAL_BACKUP — no full on record, take it now. pgbackrest backup
  # brackets pg_backup_start/stop and waits for the closing WAL to archive
  # before declaring success, so a broken archive_command fails the backup
  # loudly instead of producing an unrestorable base — no need to gate on
  # "archive-push has worked once". Earlier the gate cost 60-120s of dead
  # time on idle DBs (heartbeat → archive_timeout → archive-push cycle).
  if [ -z "$last_full" ]; then
    DECIDED_ACTION="full"; return 0
  fi

  # Catalog verification — periodically confirm S3 actually has a full backup.
  # Catches divergence between local state and S3 reality: the backup command
  # may have returned exit 0 without committing catalog metadata (S3 partial
  # write, stanza-create race), or a volume survived a redeployment with stale
  # state pointing at a different stanza/sysid path. Only clears state when the
  # catalog explicitly confirms "no backup" (exit 0 + empty backup list); an
  # unreachable S3 or missing stanza returns non-zero and is treated as
  # inconclusive so we don't burn a full on every transient S3 hiccup.
  local last_catalog_verify
  last_catalog_verify=$(read_state last_catalog_verify_at)
  local needs_verify=0
  if [ -z "$last_catalog_verify" ] || [ $((now - last_catalog_verify)) -ge "$CATALOG_VERIFY_INTERVAL_SECONDS" ]; then
    needs_verify=1
  fi
  if [ "$needs_verify" -eq 1 ]; then
    write_state_field last_catalog_verify_at "$now"
    log "verifying S3 catalog has full backup"
    catalog_check_backup
    local _crc=$?
    if [ "$_crc" -eq 0 ]; then
      log "catalog verified — full backup present in S3"
    elif [ "$_crc" -eq 2 ]; then
      log "catalog check inconclusive (S3 unreachable or stanza not yet created); skipping"
    else
      log "catalog shows no full backup despite local state (last_full=${last_full}); clearing last_full_at to trigger new full"
      write_state_field last_full_at ""
      DECIDED_ACTION="full"; return 0
    fi
  fi

  # Gap recovery — drop marker (touched by archive-push wrapper on hard
  # failure OR by check_lsn_lag_and_mark_gap when async-side queue-max-trip
  # is inferred from LSN lag) OR failed_count grew since last full. Any of
  # the three indicates archive-push had problems since the last
  # LSN-coordinated baseline, so a fresh full re-anchors the PITR window.
  local has_gap=0
  [ -f "$GAP_MARKER" ] && has_gap=1
  [ "$FAILED_COUNT" -gt "$last_full_failed" ] && has_gap=1

  if [ "$has_gap" -eq 1 ]; then
    if gap_recovered "$now" "$LAST_FAILED_EPOCH"; then
      DECIDED_ACTION="full"; return 0
    fi
    return 0  # gap still open, waiting for grace
  fi

  # Periodic full. FULL_INTERVAL_SECONDS=0 disables the periodic full while
  # still allowing NEEDS_INITIAL_BACKUP (above) and gap-recovery to fire.
  if [ "$FULL_INTERVAL_SECONDS" -gt 0 ] \
     && [ "$now" -ge $((last_full + FULL_INTERVAL_SECONDS)) ]; then
    DECIDED_ACTION="full"; return 0
  fi

  # Periodic diff.
  if [ "$DIFF_INTERVAL_SECONDS" -gt 0 ]; then
    local diff_anchor="${last_diff:-$last_full}"
    if [ "$now" -ge $((diff_anchor + DIFF_INTERVAL_SECONDS)) ]; then
      DECIDED_ACTION="diff"; return 0
    fi
  fi
}

# Emits one transactional commit right after a successful backup so the PITR
# picker has a commit-timestamp anchor to clamp `recovery_target_time`
# against. Without this, a brand-new cluster with a base backup but zero
# user commits leaves `pg_last_committed_xact()` and
# `pg_xact_commit_timestamp(newest_commit_ts_xid from pg_control_checkpoint())`
# both NULL — the picker has no safe ceiling and any restore target FATALs
# recovery with "recovery ended before configured recovery target was
# reached" (it only stops at XLOG_XACT_COMMIT records).
#
# transactional=true produces a real XLOG_XACT_COMMIT record with a commit
# timestamp, populates `pg_commit_ts/`, and the next checkpoint persists
# `newest_commit_ts_xid` into pg_control. The picker's GREATEST-of-two-
# sources query picks it up on the next 30s probe refresh.
#
# Idempotent: every subsequent backup re-fires the emit. If the cluster
# already has user commits, the extra anchor is invisible noise (one trivial
# transaction, no table side effect). Failure is non-fatal — `pg_logical_emit_message`
# only fails on a postmaster shutdown or a write barrier, in which case the
# next iteration's backup will retry.
emit_pitr_anchor() {
  psql -U postgres -tAXq -c \
    "SELECT pg_logical_emit_message(true, 'rwy_pitr_anchor', '')" \
    >/dev/null 2>&1 \
    && log "pitr anchor emitted" \
    || log "pitr anchor emit failed (non-fatal)"
}

# Emits a few bytes of WAL with no table side-effects so archive_timeout=60
# has something to flush on idle DBs. transactional=false bypasses txn
# context — non-blocking, cheap. Failure is non-fatal: a temporarily blocked
# emit just postpones the next segment switch by one tick.
emit_wal_heartbeat() {
  [ "${WAL_HEARTBEAT_DISABLED:-0}" = "1" ] && return 0
  psql -U postgres -tAXq -c \
    "SELECT pg_logical_emit_message(false, 'rwy_pitr_heartbeat', '')" \
    >/dev/null 2>&1 || true
}

watcher_iteration() {
  if ! pg_isready -h 127.0.0.1 -p 5432 -U postgres -q 2>/dev/null; then
    log "iteration skipped: pg_isready=fail (postgres not yet listening on TCP)"
    return 0
  fi
  if is_standby; then
    log "iteration skipped: standby"
    return 0
  fi

  emit_wal_heartbeat

  if ! refresh_archiver_stats; then
    log "iteration skipped: pg_stat_archiver query failed (transient psql error)"
    return 0
  fi

  # Detect silent async failure modes (queue-max-trip with no failed_count
  # bump, stuck async worker holding the lock, …) by comparing the WAL
  # segment Postgres handed off against the S3 catalog high-water. Throttled
  # internally to WAL_LAG_PROBE_INTERVAL_SECONDS so we don't S3-round-trip
  # every iteration.
  check_lsn_lag_and_mark_gap

  decide_action
  if [ -z "$DECIDED_ACTION" ]; then
    # Surface why decide_action stayed silent so post-mortems on "watcher
    # ran for N minutes and never took a backup" don't require guessing.
    log "iteration: no action (last_full=${LAST_FULL_DIAG:-?}, archived=${ARCHIVED_COUNT:-?}, failed=${FAILED_COUNT:-?}, gap_marker=${GAP_MARKER_DIAG:-?}, last_full_failed=${LAST_FULL_FAILED_DIAG:-?}, lag=${LAST_OBSERVED_LAG_SEGMENTS:-?})"
    return 0
  fi

  run_backup "$DECIDED_ACTION" || true
}

# wrapper.sh forks us unconditionally; bail silently if archiving isn't on.
# A fork has both WAL_ARCHIVE_* (own bucket / repo1) and WAL_RECOVER_FROM_*
# (source bucket / repo2). The watcher targets only repo1 (run_backup pins
# --repo=1), so the fork archives normally from boot — no skip path.
[ -z "${WAL_ARCHIVE_BUCKET:-}" ] && exit 0

# Per-cluster repo-path: read the marker (written by pgbackrest-init.sh
# during initdb, or by wrapper.sh's bootstrap subshell on existing volumes).
# pgbackrest backup needs to target the same path that archive-push is
# pushing to, otherwise stanza-create / backup land at the wrong prefix.
# The marker may not exist yet on the very first watcher iteration (we're
# forked from wrapper.sh before exec'ing docker-entrypoint), so the loop
# below re-reads it on every iteration as a cheap fallback.
sync_repo_path_from_marker() {
  if [ -f "$PGDATA/.pgbackrest_repo_path" ]; then
    PGBACKREST_REPO1_PATH=$(cat "$PGDATA/.pgbackrest_repo_path")
    export PGBACKREST_REPO1_PATH
  fi
}

sync_repo_path_from_marker

log "starting (poll=${POLL_INTERVAL_SECONDS}s, initial_poll=${INITIAL_POLL_SECONDS}s, full=${FULL_INTERVAL_SECONDS}s, diff=${DIFF_INTERVAL_SECONDS}s, gap_grace=${GAP_RESOLVED_GRACE_SECONDS}s, lag_probe=${WAL_LAG_PROBE_INTERVAL_SECONDS}s, lag_threshold=${WAL_LAG_GAP_THRESHOLD_SEGMENTS} segments, repo1-path=${PGBACKREST_REPO1_PATH:-unset})"

while true; do
  sync_repo_path_from_marker
  watcher_iteration
  if [ -z "$(read_state last_full_at)" ]; then
    sleep "$INITIAL_POLL_SECONDS"
  else
    sleep "$POLL_INTERVAL_SECONDS"
  fi
done

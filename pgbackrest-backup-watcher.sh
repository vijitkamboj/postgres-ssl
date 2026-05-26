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
#   2. Gap recovery — a state machine that fires whenever WAL coverage is
#      diverging from the S3 catalog, regardless of cause. Entry conditions
#      (any of):
#        - pgbackrest-archive-push-wrapper.sh dropped a segment and touched
#          .pgbackrest_gap_pending (bucket gone, hard archive-push failure)
#        - LSN-lag probe found pg_stat_archiver.last_archived_wal more than
#          WAL_LAG_GAP_THRESHOLD_SEGMENTS ahead of the catalog max (async
#          worker silently wedged — queue-max-trip or hung connection)
#      Recovery flow once in the state:
#        - Wait GAP_RECOVERY_BACKOFF_SECONDS (default 10 min) for natural
#          async recovery. Most short Tigris/S3 hiccups self-heal here.
#        - Still no catalog progress → pkill the async daemon. Foreground
#          archive-push respawns it on the next WAL switch. Cycle repeats
#          every GAP_RECOVERY_BACKOFF_SECONDS until catalog advances OR
#          the postgres process exits.
#        - Catalog max > catalog max at detection (proof async pushed to S3
#          successfully) → take a diff backup to re-anchor latestRestorableAt,
#          then clear the gap marker.
#      Diff (not full) is enough because the customer-visible state is
#      "latestRestorableAt is fresh", which a diff produces in seconds.
#      Historical missing segments stay missing in the old chain; retention
#      eventually rolls them off.
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
# archive-push wrapper never sees a non-zero exit.
#
# Detection: every iteration, compare pg_stat_archiver.last_archived_wal
# against the catalog max from `pgbackrest info --output=json` (parsed with
# jq for robustness — earlier grep+sort+sed extraction had a silent catch-all
# that collapsed unparseable output into "lag=0", missing real wedges). When
# lag ≥ WAL_LAG_GAP_THRESHOLD_SEGMENTS (default 32 ≈ 512 MiB) the watcher
# enters the gap-recovery state machine — see the "Gap recovery" entry in
# the file header for the full flow.

set -u

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
STATE_FILE="$PGDATA/.pgbackrest_backup_state"
GAP_MARKER="$PGDATA/.pgbackrest_gap_pending"

# POLL_INTERVAL_SECONDS / GAP_RECOVERY_BACKOFF_SECONDS are env-overridable so
# the e2e harness can exercise gap-recovery in <1 min instead of 10+. The
# defaults are conservative; nothing user-facing advertises these knobs.
POLL_INTERVAL_SECONDS="${WAL_BACKUP_POLL_INTERVAL_SECONDS:-60}"

# Until the first full lands the loop polls on a tighter cadence so a race
# with wrapper.sh's bootstrap stanza-create (or a slow first postmaster
# bind) doesn't cost a full minute per retry. After that, normal cadence.
INITIAL_POLL_SECONDS="${WAL_BACKUP_INITIAL_POLL_SECONDS:-5}"

# Cooldown between gap-recovery actions. After initial detection, the state
# machine waits this long for natural async recovery before kicking the async
# daemon. Each subsequent pkill cycle also waits this long before the next
# pkill. Catalog advance breaks out of the wait immediately.
GAP_RECOVERY_BACKOFF_SECONDS="${WAL_BACKUP_GAP_RECOVERY_BACKOFF_SECONDS:-600}"

FULL_INTERVAL_HOURS="${WAL_BACKUP_FULL_INTERVAL_HOURS:-168}"
DIFF_INTERVAL_HOURS="${WAL_BACKUP_DIFF_INTERVAL_HOURS:-24}"

# How often to verify the S3 catalog actually contains a full backup (seconds).
# Catches divergence between local state (last_full_at) and S3 reality — e.g.
# the backup command returned exit 0 but the catalog write never completed, or a
# volume survived a redeployment with stale state pointing at a different stanza
# path. 0 disables periodic verification (NEEDS_INITIAL_BACKUP still fires on
# fresh state). Default: 3600 (1 hour).
CATALOG_VERIFY_INTERVAL_SECONDS="${WAL_BACKUP_CATALOG_VERIFY_INTERVAL_SECONDS:-3600}"

# LSN-lag detection — see file header. Detection runs every iteration (no
# probe throttle): `pgbackrest info` is local-to-S3 round-trip, ~50-200ms,
# cheap enough to call every minute. 32 segments ≈ 512 MiB — far enough above
# the steady-state hand-off-vs-upload skew to avoid false positives, far
# enough below archive-push-queue-max (default 5 GiB / 320 segments) to leave
# headroom for the recovery state machine to act before the queue actually
# trips and drops segments.
WAL_LAG_GAP_THRESHOLD_SEGMENTS="${WAL_LAG_GAP_THRESHOLD_SEGMENTS:-32}"

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
#   last_lag_detected_at=<epoch>     — when current gap-recovery cycle started
#   catalog_max_at_detection=<wal>   — catalog max segment at detection (recovery
#                                       proof is "current catalog_max > this")
#   last_force_recovery_at=<epoch>   — last time we pkill'd the async daemon
#   force_attempts=<int>             — pkill cycles this gap-recovery cycle
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
      # clear_gap_recovery_state refreshes pg_stat_archiver and writes
      # last_full_failed_count itself — folds failures-during-backup into
      # the anchor so the next iteration doesn't re-fire detection.
      clear_gap_recovery_state "cleared by full backup"
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
# Returns three distinct states:
#   0 — full backup confirmed present
#   1 — conclusively no full backup (pgbackrest exit 0, structured output
#       parsed cleanly, no `.backup[]?.type == "full"` entry)
#   2 — inconclusive (pgbackrest exit non-zero, output empty, or jq parse
#       error — S3 unreachable, auth failure, stanza not yet created, etc.)
#       Caller must NOT clear local state on rc=2.
#
# Uses jq for structured navigation rather than grep `'"type":"full"'`.
# The grep would false-positive if a future pgbackrest schema (or a key in
# a sibling section that happens to be named "type" with value "full")
# matched the literal; jq with -e returns exit 1 when the filter yields
# null/empty/false, exit 2 on parse error.
catalog_check_backup() {
  local info_out rc
  info_out=$(timeout 60 pgbackrest --stanza=main --repo=1 info --output=json 2>/dev/null)
  rc=$?
  [ "$rc" -ne 0 ] && return 2
  [ -z "$info_out" ] && return 2
  if printf '%s' "$info_out" | jq -e '[.[]?.backup[]? | select(.type == "full")] | length > 0' >/dev/null 2>&1; then
    return 0
  fi
  local jq_rc=$?
  # jq exit 1 = filter evaluated to false → no full present (conclusive).
  # Any other rc (2 = parse error, 3+ = other) → treat as inconclusive.
  [ "$jq_rc" -eq 1 ] && return 1
  return 2
}

# LAST_OBSERVED_LAG_SEGMENTS / LAST_LAG_REPO_MAX surface the most recent
# observation to watcher_iteration's diagnostic log line.
LAST_OBSERVED_LAG_SEGMENTS=0
LAST_LAG_REPO_MAX=""
GAP_STATE_DIAG="clear"

# SEGMENTS_PER_LOG_FILE = 0x100000000 / wal_segment_size. Default 256 (=
# 16 MiB segsize × 256 = 4 GiB per logical log). Postgres allows
# wal_segment_size to be set at initdb between 1 MiB and 1 GiB (powers of 2),
# so a non-default segsize would otherwise miscompute lag by a factor of
# (default / actual). refresh_wal_segment_size() probes pg_settings on
# first iteration and caches; failover to a cluster with a different
# segsize (uncommon but legal) re-probes once per iteration. Cheap: it's
# one psql round-trip against a local socket on top of refresh_archiver_stats.
SEGMENTS_PER_LOG_FILE=256
WAL_SEGMENT_SIZE_BYTES=16777216

# Query pg_settings for wal_segment_size (reported in 8 KiB pages, the
# PGC_INTERNAL unit) and derive the segments-per-XLogId divisor. Sets
# SEGMENTS_PER_LOG_FILE (and WAL_SEGMENT_SIZE_BYTES for log line clarity).
# Failure is non-fatal: globals keep their last value (or the 16 MiB
# default) so segment_to_number doesn't crash; the resulting lag may be
# scaled wrong by a factor of 2 until the next successful probe, which is
# strictly less bad than not detecting wedges at all.
refresh_wal_segment_size() {
  local pages
  pages=$(psql -U postgres -tAXq -c \
    "SELECT setting::bigint FROM pg_settings WHERE name = 'wal_segment_size'" \
    2>/dev/null) || return 1
  [ -z "$pages" ] && return 1
  case "$pages" in
    *[!0-9]*) return 1 ;;
  esac
  [ "$pages" -le 0 ] && return 1
  local bytes=$(( pages * 8 * 1024 ))
  # 0x100000000 = 4294967296. Divisor must evenly divide it for any legal
  # wal_segment_size (postgres enforces power-of-2 between 1 MiB and 1 GiB
  # at initdb).
  local per_log=$(( 4294967296 / bytes ))
  [ "$per_log" -le 0 ] && return 1
  SEGMENTS_PER_LOG_FILE="$per_log"
  WAL_SEGMENT_SIZE_BYTES="$bytes"
  return 0
}

# 24-char hex WAL filename → absolute segment count. SEGMENTS_PER_LOG_FILE
# is sourced from postgres's wal_segment_size GUC, not hardcoded, so a
# cluster initdb'd with --wal-segsize=32 (or 1, or 1024) computes lag
# correctly. Echoes empty on malformed input so callers short-circuit.
# Strict shape check before the arithmetic avoids letting a stray non-hex
# character feed `$((16#…))` and crash the watcher via set -u +
# arithmetic failure.
segment_to_number() {
  local wal="$1"
  [ ${#wal} -eq 24 ] || return 0
  case "$wal" in
    *[!0-9A-Fa-f]*) return 0 ;;
  esac
  local log seg
  log=$((16#${wal:8:8}))
  seg=$((16#${wal:16:8}))
  echo $((log * SEGMENTS_PER_LOG_FILE + seg))
}

# Echoes the highest archived WAL segment on the same timeline as
# LAST_ARCHIVED_WAL ("" if the catalog has no max for that timeline yet).
# Returns 0 on a successful probe (including empty-result), 1 on transient
# failure (pgbackrest info errored, JSON unparseable). The previous version
# silently collapsed unparseable output into "lag=0" — that's the bug that
# masked Nexa/Postgres-2xa1 and ERP-3.0 at 250+ segments lag while the
# watcher reported lag=0. jq either parses or fails loudly; no quiet zero.
probe_catalog_max() {
  [ -z "$LAST_ARCHIVED_WAL" ] && { echo ""; return 0; }
  local info_out
  info_out=$(timeout 30 pgbackrest --stanza=main --repo=1 info --output=json 2>/dev/null) || return 1
  [ -z "$info_out" ] && return 1

  local tl="${LAST_ARCHIVED_WAL:0:8}"
  local repo_max
  repo_max=$(printf '%s' "$info_out" | jq -r --arg tl "$tl" '
    [ .[]?.archive[]?.max // empty
      | select(type == "string")
      | select(length == 24)
      | select(startswith($tl))
    ] | max // ""
  ' 2>/dev/null)
  local jq_rc=$?
  [ "$jq_rc" -ne 0 ] && return 1

  echo "$repo_max"
  return 0
}

# Clears all gap-recovery state (marker file + state fields). Called after a
# successful diff (recovery confirmed) or full (re-anchors the baseline).
# Re-reads pg_stat_archiver to fold any failed pushes during the backup we
# just ran into the failed_count anchor — without this the next iteration
# would see FAILED_COUNT > last_full_failed_count and immediately re-fire
# detection.
clear_gap_recovery_state() {
  local reason="${1:-cleared}"
  refresh_archiver_stats || true
  write_state_field last_full_failed_count "${FAILED_COUNT:-0}"
  write_state_field last_lag_detected_at 0
  write_state_field catalog_max_at_detection ""
  write_state_field last_force_recovery_at 0
  write_state_field force_attempts 0
  if [ -f "$GAP_MARKER" ]; then
    rm -f "$GAP_MARKER"
    log "gap-recovery: ${reason}"
  fi
}

# Kicks the async daemon. Foreground archive-push respawns it on the next
# WAL switch (heartbeat + archive_timeout=60 guarantees one within ~60s).
# Crashed-daemon case: pkill is a no-op, respawn happens regardless.
# Hung-daemon case: pkill removes the stuck process so respawn can succeed.
# Spool is safe to disrupt — pgBackRest re-uploads from pg_wal on respawn.
#
# Target: the literal substring "archive-push:async" in the cmdline.
# pgBackRest spawns the async daemon via cfgExecParam(cfgCmdArchivePush,
# cfgCmdRoleAsync, ...) and cfgParseCommandRoleName (src/config/parse.c)
# encodes the role with a colon — argv[1] of the spawned process becomes
# "archive-push:async". The foreground caller (which runs as
# archive_command and exits in ~300ms) has "archive-push" *without* a
# colon, so the colon disambiguates: pkill matches the long-lived async
# daemon but never the short-lived foreground invocation. Verify in a
# running container with `pgrep -af archive-push:async`.
kick_async_daemon() {
  pkill -f 'archive-push:async' 2>/dev/null || true
}

# Recovery state machine. Replaces the old "wait for grace then take a full"
# path with: detect → wait 10 min → pkill → wait 10 min → pkill → … →
# (catalog advances) → take diff → clear. Repeats pkill every backoff window
# during an extended upstream outage; the diff fires the instant the catalog
# actually advances past the detection point, which is the only conclusive
# proof async has resumed pushing to S3.
#
# Called every iteration. Idempotent: re-entering the function while already
# in recovery just advances the timers / inspects current catalog max.
#
# Returns 0 always — the caller's decide_action checks the gap marker to
# avoid racing periodic-full/diff on top of an in-flight recovery.
gap_recovery_step() {
  local now; now=$(date +%s)

  local catalog_max
  catalog_max=$(probe_catalog_max)
  local probe_rc=$?
  if [ "$probe_rc" -ne 0 ]; then
    GAP_STATE_DIAG="probe-failed"
    log "gap-recovery: pgbackrest info probe failed; leaving state unchanged"
    return 0
  fi
  LAST_LAG_REPO_MAX="$catalog_max"

  # Lag (postgres handoff minus catalog max). 0 if either side missing.
  local lag=0
  if [ -n "$LAST_ARCHIVED_WAL" ] && [ -n "$catalog_max" ]; then
    local h_n c_n
    h_n=$(segment_to_number "$LAST_ARCHIVED_WAL")
    c_n=$(segment_to_number "$catalog_max")
    if [ -n "$h_n" ] && [ -n "$c_n" ]; then
      lag=$((h_n - c_n))
      [ "$lag" -lt 0 ] && lag=0
    fi
  fi
  LAST_OBSERVED_LAG_SEGMENTS="$lag"

  # In recovery? Marker is the truth — either we set it on a previous lag
  # detection or the archive-push wrapper touched it on a hard failure.
  if [ -f "$GAP_MARKER" ]; then
    local detected_at catalog_at_detection last_force force_attempts
    detected_at=$(read_state last_lag_detected_at); : "${detected_at:=0}"
    catalog_at_detection=$(read_state catalog_max_at_detection); : "${catalog_at_detection:=}"
    last_force=$(read_state last_force_recovery_at); : "${last_force:=0}"
    force_attempts=$(read_state force_attempts); : "${force_attempts:=0}"

    # Back-fill state when the wrapper touched the marker but the watcher
    # hasn't entered the state machine yet. Treat now as detection time and
    # the current catalog max as the baseline to beat.
    if [ "$detected_at" -eq 0 ]; then
      detected_at="$now"
      write_state_field last_lag_detected_at "$now"
    fi
    # Only write a real value; an empty catalog (fresh stanza, no archive
    # entries on this timeline yet) leaves the field unset and the back-fill
    # re-fires next iteration. Writing "" would set catalog_at_detection
    # equal to the first non-empty catalog_max captured in a later
    # iteration's back-fill, and we'd then never see a difference vs.
    # current catalog_max — recovery couldn't fire.
    if [ -z "$catalog_at_detection" ] && [ -n "$catalog_max" ]; then
      catalog_at_detection="$catalog_max"
      write_state_field catalog_max_at_detection "$catalog_at_detection"
    fi

    # Recovery proof: catalog advanced past where it was when we entered
    # the state machine. This means the async daemon successfully pushed
    # at least one segment to S3 — async is working again.
    if [ -n "$catalog_max" ] && [ -n "$catalog_at_detection" ] && [ "$catalog_max" != "$catalog_at_detection" ]; then
      local c_n_curr c_n_det
      c_n_curr=$(segment_to_number "$catalog_max")
      c_n_det=$(segment_to_number "$catalog_at_detection")
      if [ -n "$c_n_curr" ] && [ -n "$c_n_det" ] && [ "$c_n_curr" -gt "$c_n_det" ]; then
        GAP_STATE_DIAG="recovering"
        log "gap-recovery: catalog advanced (${catalog_at_detection} → ${catalog_max}, ${force_attempts} pkill cycles) — taking diff to anchor restore point"
        if run_backup diff; then
          clear_gap_recovery_state "cleared by gap-recovery diff"
          GAP_STATE_DIAG="clear"
        else
          log "gap-recovery: diff failed; retry on next iteration"
        fi
        return 0
      fi
    fi

    # No catalog advance yet. Check if it's time to kick (or kick again).
    local last_action_at="$detected_at"
    [ "$last_force" -gt "$last_action_at" ] && last_action_at="$last_force"
    local since_action=$((now - last_action_at))

    if [ "$since_action" -ge "$GAP_RECOVERY_BACKOFF_SECONDS" ]; then
      force_attempts=$((force_attempts + 1))
      local stuck_min=$(( (now - detected_at) / 60 ))
      log "gap-recovery: catalog frozen at ${catalog_at_detection} for ${stuck_min}min (handoff=${LAST_ARCHIVED_WAL}, lag=${lag}) — pkill async (attempt #${force_attempts})"
      kick_async_daemon
      write_state_field last_force_recovery_at "$now"
      write_state_field force_attempts "$force_attempts"
      GAP_STATE_DIAG="forced"
    else
      # No log line during the wait — the per-iteration "iteration: no
      # action (... gap_state=waiting ...)" diagnostic at the end of
      # watcher_iteration already surfaces enough state for operators
      # without printing the same line 10× per backoff cycle.
      GAP_STATE_DIAG="waiting"
    fi
    return 0
  fi

  # Not in recovery. Two independent entry conditions, both meaning
  # "WAL coverage is diverging from the catalog":
  #   - LSN lag ≥ threshold (async wedge / queue-max-trip — postgres
  #     keeps handing off, async doesn't drain to S3)
  #   - failed_count grew since the last full's anchor (foreground hard
  #     failure — archive_command returning non-zero so postgres never
  #     hands off; lag stays at 0 but archiving is broken just the same)
  local last_full_failed; last_full_failed=$(read_state last_full_failed_count); : "${last_full_failed:=0}"
  local failed_grew=0
  [ "${FAILED_COUNT:-0}" -gt "$last_full_failed" ] && failed_grew=1

  if [ "$lag" -ge "$WAL_LAG_GAP_THRESHOLD_SEGMENTS" ] || [ "$failed_grew" -eq 1 ]; then
    touch "$GAP_MARKER"
    write_state_field last_lag_detected_at "$now"
    write_state_field catalog_max_at_detection "$catalog_max"
    write_state_field last_force_recovery_at 0
    write_state_field force_attempts 0
    GAP_STATE_DIAG="detected"
    log "gap-recovery: entering recovery (lag=${lag}, failed_count=${FAILED_COUNT:-0} vs anchor ${last_full_failed}, handoff=${LAST_ARCHIVED_WAL}, catalog_max=${catalog_max}) — first pkill in ${GAP_RECOVERY_BACKOFF_SECONDS}s if catalog hasn't advanced"
  else
    GAP_STATE_DIAG="clear"
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

  # Gap-recovery state machine owns the .pgbackrest_gap_pending marker. While
  # the marker is present, decide_action stays silent — gap_recovery_step
  # already ran this iteration and either took a diff, kicked the async
  # daemon, or is waiting on the backoff. Racing a periodic full (or worse,
  # a catalog-verify-triggered full) on top of an in-flight recovery would
  # burn a full at the worst time (mid-outage). The marker check MUST stay
  # above catalog-verify: an hourly verify firing mid-gap-recovery against
  # a wedged S3 path can see backup.info just rotated by retention and
  # mis-conclude "no full present" → clear last_full_at → force a full,
  # which then fails through the same wedged S3.
  if [ -f "$GAP_MARKER" ]; then
    return 0
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
    log "verifying S3 catalog has full backup"
    catalog_check_backup
    local _crc=$?
    # Stamp the verify timestamp only on conclusive results (rc=0 full
    # present, rc=1 no full present). On rc=2 (inconclusive — S3 hiccup,
    # stanza not yet created) leave the timestamp untouched so the next
    # iteration retries instead of locking out the verify for an hour.
    if [ "$_crc" -eq 0 ]; then
      write_state_field last_catalog_verify_at "$now"
      log "catalog verified — full backup present in S3"
    elif [ "$_crc" -eq 2 ]; then
      log "catalog check inconclusive (S3 unreachable or stanza not yet created); skipping"
    else
      write_state_field last_catalog_verify_at "$now"
      log "catalog shows no full backup despite local state (last_full=${last_full}); clearing last_full_at to trigger new full"
      write_state_field last_full_at ""
      DECIDED_ACTION="full"; return 0
    fi
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

  # Cache wal_segment_size for segment_to_number's per-XLogId divisor.
  # Cheap (local psql round-trip) so we re-read every iteration to handle
  # the very-rare case of failing over onto a cluster with a different
  # segsize. Failure leaves the previous value in place; the watcher
  # never sees an arithmetic crash from a missing global.
  refresh_wal_segment_size || true

  # Gap-recovery state machine — detects WAL/catalog divergence and drives
  # the kick-and-diff sequence. Runs every iteration; pgbackrest info is
  # cheap enough that throttling isn't worth the false-negative window the
  # earlier throttled version introduced.
  gap_recovery_step

  decide_action
  if [ -z "$DECIDED_ACTION" ]; then
    # Surface why decide_action stayed silent so post-mortems on "watcher
    # ran for N minutes and never took a backup" don't require guessing.
    log "iteration: no action (last_full=${LAST_FULL_DIAG:-?}, archived=${ARCHIVED_COUNT:-?}, failed=${FAILED_COUNT:-?}, gap_marker=${GAP_MARKER_DIAG:-?}, gap_state=${GAP_STATE_DIAG:-?}, last_full_failed=${LAST_FULL_FAILED_DIAG:-?}, lag=${LAST_OBSERVED_LAG_SEGMENTS:-?})"
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

log "starting (poll=${POLL_INTERVAL_SECONDS}s, initial_poll=${INITIAL_POLL_SECONDS}s, full=${FULL_INTERVAL_SECONDS}s, diff=${DIFF_INTERVAL_SECONDS}s, gap_backoff=${GAP_RECOVERY_BACKOFF_SECONDS}s, lag_threshold=${WAL_LAG_GAP_THRESHOLD_SEGMENTS} segments, repo1-path=${PGBACKREST_REPO1_PATH:-unset})"

# Crash-isolate each iteration in a subshell so a future refactor accident
# (set -u trip on an unbound var, an unexpected non-zero exit, a libpq
# environment quirk, …) doesn't terminate the outer loop. The state file +
# .pgbackrest_gap_pending live on disk and survive across iterations, so a
# subshell that exits mid-recovery just resumes on the next tick. This is
# the watcher's own supervisor — keeping it inside the script (rather than
# in wrapper.sh) means the watcher's long-lived PID (the bash interpreter
# running this loop) stays pinned; only the transient iteration subshell
# churns through PIDs, and each subshell lives a few seconds. With the
# old wrapper.sh-side `while true; do gosu watcher; done` supervisor, the
# watcher's long-lived PID cycled through the same range where postgres
# lands on container start, which raced postgres's stale-postmaster.pid
# check (PID 45 from a phase-1 SIGKILL collided with a phase-2 watcher
# respawn at the same number, postgres saw it as a live postmaster and
# FATAL'd). Pinning the watcher main PID makes that race impossible.
while true; do
  (
    sync_repo_path_from_marker
    watcher_iteration
  ) || log "iteration subshell exited non-zero, continuing"
  if [ -z "$(read_state last_full_at)" ]; then
    sleep "$INITIAL_POLL_SECONDS"
  else
    sleep "$POLL_INTERVAL_SECONDS"
  fi
done

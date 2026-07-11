# CephX Daemon Key Rotation via StorageCluster

**Date:** 2026-07-11
**Status:** Approved for implementation planning
**Scope:** Daemon CephX key rotation only (CSI / rbdMirrorPeer later)

## Problem

CephX key rotation automation currently patches the Rook **CephCluster** CR:

```text
spec.security.cephx.daemon.{keyRotationPolicy,keyGeneration}
```

ODF design changed: daemon key rotation is now driven from the **StorageCluster** CR:

```text
spec.managedResources.cephCluster.security.cephx.daemon
```

On a fresh cluster, `managedResources.cephCluster.security` is unset. The first enable must create that block. The default first `keyGeneration` is **2** (not 1).

## Goals

- Drive daemon CephX rotation enable / rotate / disable through StorageCluster.
- Keep CSI and rbdMirrorPeer on the existing CephCluster path.
- Keep status verification on CephCluster / CephFilesystem (unchanged).
- Default first enable to `keyGeneration: 2` without blocking negative tests that explicitly set a lower generation.

## Non-goals

- Moving CSI / rbdMirrorPeer rotation to StorageCluster.
- Changing status wait targets or auth/pod verification logic.
- Auto-flipping `reconcileStrategy` for daemon rotation.

## Approach

Split the write path by component (Approach 1):

| Component | Spec write target |
|-----------|-------------------|
| `daemon` | StorageCluster `managedResources.cephCluster.security.cephx.daemon` |
| `csi` | CephCluster `spec.security.cephx.csi` (unchanged) |
| `rbdMirrorPeer` | CephCluster `spec.security.cephx.rbdMirrorPeer` (unchanged) |

## Spec shape

Daemon patch payload (both fields required):

```json
{
  "keyRotationPolicy": "KeyGeneration",
  "keyGeneration": 2
}
```

Full StorageCluster path example:

```json
{
  "security": {
    "cephx": {
      "daemon": {
        "keyRotationPolicy": "KeyGeneration",
        "keyGeneration": 2
      }
    }
  }
}
```

JSON-patch must add missing parents (`cephCluster` / `security` / `cephx` / `daemon`) and preserve sibling fields such as `allowedCiphers`.

## Generation rules

### Happy path (option B)

`ensure_daemon_key_rotation_enabled(key_generation=2)` and `get_next_*` helpers:

- Fresh / unset security → write policy + `keyGeneration: 2`.
- If current generation (spec or status) is already ≥ requested → preserve / use the higher value.
- Later rotations → `max(StorageCluster daemon spec, relevant status) + 1`.

### Low-level write (negative-test friendly)

`rotate_component_keys(..., key_generation=<explicit>)` and the StorageCluster patch helper:

- Write the caller-provided generation as-is.
- Do **not** clamp or reject decreases — negative tests may set a lower value intentionally.

## Helper changes (`ocs_ci/helpers/cephx_keyrotation_helper.py`)

1. Update module docstring: daemon rotation is StorageCluster-driven; CSI/rbdMirrorPeer remain CephCluster-driven.
2. Add StorageCluster helpers to get/patch `managedResources.cephCluster.security.cephx.daemon` (same parent-add pattern as `patch_storagecluster_allowed_ciphers`).
3. Route daemon paths in:
   - `rotate_component_keys` (when component is `daemon`)
   - `ensure_daemon_key_rotation_enabled` (default `key_generation=2`)
   - `get_spec_key_generation` / `get_spec_rotation_policy` for daemon
   - `disable_component_key_rotation` for daemon
4. Leave CSI / rbdMirrorPeer on `_build_cephx_component_patch_ops` → CephCluster.
5. Status waits (`wait_for_*`, auth/pod checks) unchanged.

## Fixture & test updates

- `tests/functional/cephx_keyrotation/conftest.py`: `ensure_daemon_key_rotation_enabled(key_generation=2)`.
- `test_cephx_keyrotation_configuration.py`: `EXPECTED_INITIAL_GENERATION = 2`.
- Policy-disabled tests: daemon disable via StorageCluster; CSI/rbdMirrorPeer disable still CephCluster.
- Daemon rotation / negative / metrics / lockbox tests: no wait-API changes; they pick up the new rotate path automatically.
- Krkn `background_cluster_operations` continues calling `rotate_rook_daemon_keys()` / `rotate_component_keys()` — no API change required.

## Reconciliation & errors

- After StorageCluster patch, ODF reconciles into CephCluster; existing status waits remain the completion signal.
- Helpers do not change `reconcileStrategy`.
- Patch/command failures continue to raise `CommandFailed` / `UnexpectedBehaviour`; status wait timeouts unchanged.

## Future work

Move CSI and rbdMirrorPeer to the same StorageCluster `security.cephx` path when product support lands.

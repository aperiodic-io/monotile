#!/usr/bin/env bash
# Line-coverage gates (docs/plan/06-testing.md §6.7) over an lcov report of the unit, property,
# golden and driver tests: brrrrr-core >= 95%; the runtime crate, whose Kafka and object store
# I/O only the acceptance scenarios reach, >= a floor of what its unit tests cover.
# A gate that finds no instrumented lines fails: a moved path must not silently disable it.
set -euo pipefail
report="${1:-lcov.info}"
gate() { # <path regex> <name> <minimum %>
  awk -F: -v re="$1" -v name="$2" -v min="$3" '
    /^SF:/ { mine = ($2 ~ re) }
    mine && /^LF:/ { lf += $2 }
    mine && /^LH:/ { lh += $2 }
    END {
      if (lf == 0) { print name ": no instrumented lines under " re; exit 1 }
      pct = 100 * lh / lf
      printf "%s line coverage: %.2f%% (%d/%d), gate %s%%\n", name, pct, lh, lf, min
      if (pct < min) { print name ": below the gate"; exit 1 }
    }' "$report"
}
gate 'crates/brrrrr-core/src/' brrrrr-core 95
# measured at 34% when the gate was set, 64% once the checkpoint store was unit-tested on a
# directory: raise it as the runtime's pure parts grow
gate 'crates/brrrrr/src/' brrrrr 60

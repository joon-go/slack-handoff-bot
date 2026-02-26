# Architecture Overview

## Two-Pass Scan Model

### Scan-A
Counts tickets created during regional shift window.
Filters to L1+L2 team.

### Scan-B
Computes queue metrics:
- FR SLA Pending P0/P1
- FR SLA Pending P2/P3
- Aged SLA tickets
- Open handoff tickets

## Time Handling
- Uses Luxon
- Pacific Time → UTC conversion
- DST safe

## Performance
- Cursor paging
- 30-day lookback cutoff (configurable)

# RouteViews Access Logs

The RouteViews access logs contain logs of all accesses in OSDF of the `routeviews`
namespace (per-collector paths such as `route-views6/` live beneath it).

Tthis dataset is emitted as **JSONL**
— one JSON object per line — which is easy to ingest and to query ad-hoc with `jq`.

The workflow runs daily at 00:00:00 UTC and contains accesses from the previous day.
There are two streams:

- **Cache** — per-access records for non-origin sites: `latest-cache.jsonl` (rolling) and
  `<date>-cache.jsonl` (dated).
- **Origin** — accesses at the RouteViews origin aggregated into five-minute buckets by
  object name: `latest-origin.jsonl` (rolling) and `<date>-origin.jsonl` (dated).

RouteViews volume is small (a busy day is a few thousand records, a couple of MB of raw
JSONL), so files are committed **uncompressed** as `.jsonl` — they can be fetched from the
published URL and piped straight into `jq`, e.g.:

```
curl -s https://pelicanplatform.github.io/pelican-access-logs/routeviews-access-log/latest-cache.jsonl | jq .
```

## Schema

### Cache record
| Field | Type | Meaning |
|-------|------|---------|
| `timestamp` | int | Epoch **milliseconds** of the access |
| `object_name` | string | Object path |
| `site` | string | OSDF site name |
| `remote_ip` | string \| null | Client IP (`null` when not recorded) |
| `server` | string | Serving host |
| `server_type` | string | `cache`, `origin`, or `unknown` |
| `latitude` | float | Server latitude (0 if unknown) |
| `longitude` | float | Server longitude (0 if unknown) |
| `appinfo` | string | Raw client user-agent — includes the **client version** |
| `pelican_client` | bool | True **only** for the Pelican client (`appinfo` contains `pelican-client` or `xrdcl-pelican`); all other clients — Python/pelicanfs, `xrdcl-curl`, curl, browsers — are false |
| `bytes_sent` | int | Bytes read/served (OSDF `read`) |
| `bytes_rcvd` | int | Bytes written (OSDF `write`) |
| `op_time` | int | Raw source `operation_time`, in **seconds** — this is xrootd server uptime, NOT a transfer duration (see note below) |

Example:
```json
{"timestamp":1751843559026,"object_name":"/route-views6/bgpdata/2026.07/UPDATES/updates.20260701.1600.bz2","site":"AMST_INTERNET2_OSDF_CACHE","remote_ip":"84.88.185.81","server":"127.0.0.1","server_type":"cache","latitude":52.17,"longitude":4.4728,"appinfo":"Python-urllib/3.12","pelican_client":false,"bytes_sent":2634022912,"bytes_rcvd":0,"op_time":55621}
```

### Origin record (aggregated, five-minute buckets)
| Field | Type | Meaning |
|-------|------|---------|
| `timestamp` | int | Epoch milliseconds of the 5-minute bucket |
| `object_name` | string | Object path |
| `site` | string | Always the RouteViews origin site |
| `server_type` | string | `origin` |
| `bytes_sent` | int | Summed bytes read |
| `bytes_rcvd` | int | Summed bytes written |
| `op_time` | int | Summed source `operation_time`, in seconds (server uptime, not transfer time) |
| `count` | int | Number of accesses aggregated into the bucket |

Example:
```json
{"timestamp":1751760000000,"object_name":"/routeviews/route-views6/bgpdata/...","site":"KENNESAW_OSSTORE_PUBLIC","server_type":"origin","bytes_sent":24248320,"bytes_rcvd":0,"op_time":0,"count":185}
```

## Notes
- **`timestamp`** in the output is always normalized to epoch **milliseconds** — it is derived
  from the source `@timestamp` field (ISO-8601, always present), which is the one time field
  that is consistent across all record producers.
- **`op_time` is the raw source `operation_time`, is in seconds, and is NOT a per-transfer
  duration — it is the serving xrootd instance's uptime.** In the source
  `operation_time == end_time - start_time`, where (verified against the July 2026 data):
    - `end_time` equals the record's `@timestamp` in epoch **seconds** (e.g. `1783706559` →
      `2026-07-10T18:02:39Z`); read as milliseconds it would be 1970, so the unit is seconds.
    - `start_time` is byte-identical to the leading field of `serverID`
      (`<serverStartTime>#<host>:<port>`), i.e. the xrootd server instance's start time — not a
      per-request start. Consequently many distinct transfers from the same server session share
      an identical `operation_time` (server uptime, often 10-30 days).
  There is no wall-clock transfer-duration field in the source records, so there is no
  equivalent to an Apache-style `us_taken`. Do not use `op_time` as a transfer time. This holds
  for both streams: a 30-day sample of 600 cache-side records across 22 cache sites (including
  `AMST_INTERNET2_OSDF_CACHE`) had `start_time == serverID` server-boot stamp in 100% of cases.
  A true per-transfer duration is not recoverable from the GRACC OpenSearch (checked
  `xrd-stash*`, `gracc.osg.summary*`, `gracc.osg.raw*`); it would require XRootD detailed
  "f-stream" (fstat) monitoring, which is not ingested here — a question for the OSG/OSDF
  monitoring operators.
  (Some non-RouteViews OSDF producers emit `start_time`/`end_time` in milliseconds, so the raw
  unit is source-dependent across the wider index; for RouteViews records it is seconds.)
- The origin stream is filtered to the `routeviews/` namespace. `KENNESAW_OSSTORE_PUBLIC` is a
  shared origin that also serves large volumes of `/pelican/monitoring/` health-check traffic
  (~19k probes/day vs ~850 real accesses on the sampled day); without the namespace filter the
  origin log would be almost entirely monitoring noise.
- Namespace prefix (`routeviews/`) and origin site name (`KENNESAW_OSSTORE_PUBLIC`) are set
  in `.github/scripts/routeviews-access-log/main.py`.
- The RouteViews contact's own Apache logs use additional fields (`is_bz2`, `url`,
  `end_status`, `user_agent`, `us_taken`, `exit`). Those are not present in the OSDF
  OpenSearch data, so this dataset emits the OSDF-derived mapping above (the schema the
  contact proposed).

# study-note-deploy-system-observe

관측 서버 — 네 서비스(front · backend · llm · 배포 시스템)가 Redis 스트림 `logs` 로 보내는 요청 단위 로그를 받아 30일 동안 보관하고, 조회 API 로 찾아보게 한다. 어드민의 로그인 시도를 기록하고 잠금을 판정한다(아래 "로그인 시도와 잠금"). 서버 셋의 자원 지표를 받아 보관하고 조회하게 한다(아래 "자원 지표"). Kotlin · Spring Boot · PostgreSQL.

```
front · backend · llm · 배포 시스템 ── XADD logs ──▶ Redis(backend 서버의 것, 상한 약 1만 건)
                                                          │ 소비자 그룹으로 읽기 → 저장 → 확인(ack)
                                                          ▼
                       어드민 · 알림 서버 ── GET /logs · /logs/stats · /health ──▶ 관측 서버 ──▶ PostgreSQL(같은 compose, 30일 보관)
```

## 수집

- `logs` 스트림을 소비자 그룹(`observe`)으로 읽는다. **저장에 성공한 뒤에만 확인(ack)한다** — 저장에 실패하거나 관측 서버가 죽으면 확인하지 못한 항목이 남고, 다음에 그것부터 다시 받는다.
- 같은 항목을 두 번 받아도 한 번만 저장된다(스트림 ID 가 기본 키).
- 한 줄 `<요청ID>:<서비스>:<내용>` 을 나눠 저장한다. 나눌 수 없는 줄은 버리지 않고 서비스 `unknown`·원문 그대로 저장한다. 길이는 자르지 않는다(PostgreSQL 이 받지 못하는 NUL 문자만 지운다).
- 로그 시각은 스트림 ID 의 밀리초(Redis 가 받은 시각)다.
- 30일 지난 행은 매일 03:40(서울)에 지운다.
- **잃는 범위**: 전제는 **수집이 스트림이 버티는 시간(상한 약 1만 건, 평소 약 9시간치)보다 오래 멈추지 않는다**는 것이다. 수집이 멈추는 경우는 관측 서버가 꺼졌을 때와, 떠 있어도 DB 에 저장하지 못할 때(DB 불통·디스크 가득) 둘 다다. 전제 안에서는 잃지 않는다.
  - 전제가 깨지면 그 사이 상한을 넘어 밀려난 로그를 잃는다 — 이 상태는 `/health` 503 으로 알림 서버가 잡고, 밀려나 잃은 건수는 경고 로그(`unacknowledged entries were already trimmed from the stream — lost`)에 남는다.
  - Redis 가 재시작하면 스트림에 남아 있던 미수집분도 잃는다(디스크 저장이 없다).
- 발신 쪽(네 서비스의 로거·형식·스트림 상한)과 Redis 설정은 바꾸지 않는다. 관측 서버가 죽어도 네 서비스의 요청 처리와 로그 발신은 영향이 없다.
- 수집·조회·헬스는 DB 연결을 나눠 쓴다 — 조회가 무겁거나 헬스 요청이 몰려도 수집이 밀리지 않는다.

## API

| 경로 | 인증 | 설명 |
|---|---|---|
| `GET /logs` | `X-Observe-Secret` | 로그 조회(최신순). 조건은 아래 |
| `GET /logs/stats` | `X-Observe-Secret` | 기간 안의 시간·서비스·레벨별 건수(`counts: [{hour, service, level, count}]`). 기간을 주지 않으면 최근 24시간 |
| `GET /health` | 없음 | 수집이 살아 있는가 — 아래 "헬스" |
| `POST /auth/attempts` | `X-Observe-Secret` | 로그인 시도의 기록과 잠금 판정 — 아래 "로그인 시도와 잠금" |
| `POST /auth/attempts/rejected` | `X-Observe-Secret` | 자격 검증 전에 거절한 시도의 기록 |
| `GET /auth/attempts` | `X-Observe-Secret` | 시도 기록 목록(최신순) |
| `GET /auth/lock` | `X-Observe-Secret` | 지금의 잠금 상태(화면 표시용) |

`GET /logs` 의 조건(모두 선택):

| 이름 | 뜻 |
|---|---|
| `from` · `to` | ISO 시각(예: `2026-11-21T00:00:00Z`). 둘 다 없으면 최근 1시간, `from` 만 있으면 지금까지, `to` 만 있으면 그 앞 1시간. 최대 31일 |
| `service` | 서비스 이름(`a-z0-9-`, 40자 이하) |
| `level` | `debug` · `info` · `warning` · `error` |
| `request_id` | 요청ID(`A-Za-z0-9_.-`, 80자 이하) |
| `q` | 내용 부분 일치. 2자 이상(앞뒤 공백 제외) 200자 이하. 2자 검색어는 인덱스(trigram, 3자 단위)로 좁히지 못해 드문 말이면 전체를 훑는다(75만 행 실측 0.45~1.0초). 3자 이상은 1초 안 |
| `limit` | 1~500, 기본 100 |
| `before` | 이어 보기 — 앞 응답의 `next_before` 를 넘기고, **앞 응답의 `from`·`to` 도 그대로 함께 넘긴다**. `next_before` 가 빈 문자열이면 끝. `from`·`to` 없이 `before` 만 넘기면 422 — 쪽마다 "지금부터 1시간"을 다시 세면 창의 오래된 끝이 밀려 그 사이 로그가 빠진다 |

- 조건이 틀리면 422(`invalid_request`, `detail` 에 이유), 비밀값이 없거나 다르면 401. 서버의 `OBSERVE_SECRET` 이 비어 있으면 조회를 전부 거절한다.
- 조회 중 DB 오류는 503(`query_failed`), 조회 시간 초과(10초, statement_timeout)는 504(`query_timeout`) — `/logs`·`/logs/stats` 둘 다. 예외 문구는 응답에 넣지 않고 서버 로그에만 남긴다.
- 응답: `{success, data: {logs: [{id, at, level, service, request_id, message}], next_before, from, to}}`. 실패: `{success: false, error: {code}}`.
- 이어 보기 예: `GET /logs?service=backend` → 응답의 `from`·`to`·`next_before` 로 `GET /logs?service=backend&from=<from>&to=<to>&before=<next_before>` … `next_before` 가 빌 때까지.

### 헬스

`GET /health` 는 물을 때마다 Redis(미확인·밀린 건수)와 DB(가장 최근 행의 시각)에 직접 묻는다 — 결과를 담아 두지 않아 DB 가 끊기면 다음 요청이 바로 503 이다. DB 확인은 헬스 전용 연결(1개)로 해 헬스가 몰려도 수집과 다투지 않는다. **200 은 `ok` 하나뿐이다.**

| `status` | HTTP | 뜻 |
|---|---|---|
| `ok` | 200 | 마지막으로 성공한 수집 바퀴가 60초 안(읽을 것이 없었어도) |
| `starting` | 503 | 뜬 지 60초 안, 아직 한 바퀴도 성공하지 못함 — 배포의 헬스 게이트가 한 번도 수집하지 못한 자리를 통과시키지 않게 |
| `failing` | 503 | 마지막 수집 바퀴가 실패(저장 실패·Redis 오류, 스키마 생성 실패는 `failure` 가 `schema …`) |
| `unreachable` | 503 | 지금 Redis 나 DB 에 닿지 못함(`failure` 가 `redis …` · `database …`). 표가 아직 없다는 DB 의 답은 닿은 것으로 보아 넣지 않는다 — 스키마를 만들기 전은 `starting`, 만들지 못했으면 `failing` 으로 보인다 |
| `stalled` | 503 | 수집 바퀴가 60초 넘게 끝나지 않음(첫 바퀴 전 포함) |

판정 순서는 `unreachable` → `failing` → `ok` → `starting` → `stalled`.

| 본문 칸 | 뜻 |
|---|---|
| `status` | 위 표 |
| `last_loop_at` | 마지막으로 성공한 수집 바퀴가 끝난 시각(없으면 빈 문자열) |
| `last_stored_at` | DB 의 가장 최근 로그가 찍힌 시각 — DB 에서 읽어 재시작 직후에도, 다른 자리가 저장한 것도 보인다(표가 비었거나 아직 없거나 DB 에 닿지 못하면 빈 문자열) |
| `stored_since_start` | 이 프로세스가 뜬 뒤 저장한 건수 |
| `unacknowledged` | 받았지만 확인(ack)하지 못한 건수(물어볼 수 없으면 -1) |
| `lag` | 그룹에 아직 전달되지 않은 밀린 건수(`XINFO GROUPS` 의 lag, 읽지 못하면 -1). 판정에는 넣지 않는다 — 밀림은 진행 상황 |
| `failure` | 실패 사유 — 예외 종류 이름만(헬스는 인증이 없어 문구는 서버 로그에만). 정상이면 빈 문자열 |
| `last_retention_at` · `last_retention_deleted` · `last_retention_failure` | 마지막 30일 삭제의 실행 시각 · 지운 건수 · 실패 종류(뜬 뒤 아직 안 돌았으면 빈 문자열 · -1 · 빈 문자열, 실패면 건수 -1). 판정에는 넣지 않는다 |
| `last_auth_retention_at` · `last_auth_retention_deleted` · `last_auth_retention_failure` | 마지막 로그인 시도 기록 삭제(보관 기간, 기본 90일)의 같은 세 칸. 판정에는 넣지 않는다 |
| `log_alerts` | 로그 무수신 판정의 칸(아래 "로그 무수신 알림") — 판정의 어떤 실패도 `status` 와 HTTP 상태를 바꾸지 않는다 |

## 로그인 시도와 잠금

어드민(계정 하나)의 로그인 시도를 기록하고, 연속 실패에 따른 잠금을 판정한다. 어드민 자리가 둘이어도 같은 답을 보게 판정은 여기서 한다.

- **잠금**: 연속 `OBSERVE_AUTH_FAILURE_LIMIT`(기본 5)회 실패 → `OBSERVE_AUTH_LOCK_DURATION`(기본 15분) 잠금. 잠금 중에는 자격이 맞아도 `locked` 로 거절하고, 그 시도는 횟수를 늘리지도 잠금을 연장하지도 않는다(기록만). 성공하면 횟수 0. 잠금 끝 시각이 지나면 0 부터 센다. 시각은 요청의 `at` 기준이다.
- **판정은 PostgreSQL 트랜잭션 한 건**: 잠금 상태 행을 `SELECT … FOR UPDATE` 로 잡고 기록 · 횟수 갱신 · 판정을 함께 커밋한다. 동시에 들어온 실패도 횟수가 겹치지 않고 잠금 시작(`lock_started:true`)은 한 번뿐이다.
- **같은 `attempt_id`** 를 다시 보내면 새로 세지 않고 처음의 판정·처음의 기록을 그대로 돌려준다(응답을 잃은 재시도용 — 동시 재전송도 같다). 거절 기록도 같은 ID 면 한 건.
- **보관**: `OBSERVE_AUTH_RETENTION`(기본 90일). 30일 로그 삭제와 같은 예약 실행(매일 03:40 서울)에서 따로 돈다 — 한쪽이 실패해도 다른 쪽은 돌고, 결과는 헬스의 `last_auth_retention_*` 와 서버 로그에 남는다.
- **표**: `auth_attempt`(시도 한 건 = 한 행) · `auth_lock_state`(잠금 상태 한 행). `log_entry` 는 건드리지 않는다. 표는 **관측 서버가 뜬 뒤 이 경로들에 처음 닿는 호출(또는 예약 삭제)이 만든다**(`CREATE … IF NOT EXISTS`) — 뜰 때 DB 가 없어도 앱은 죽지 않고, 만들지 못한 동안의 호출은 503 이며 다음 호출이 다시 만든다. 두 자리가 동시에 처음 만들면 한쪽 호출 하나가 실패하고 다음 호출에 회복한다.
- **연결**: 전용 연결 풀(3개, 문장 4초 제한) — 수집·로그 조회와 다투지 않는다.
- 입력한 ID·비밀번호는 받지도 저장하지도 않는다(자격이 맞았는가만). IP·User-Agent 는 이 표에만 있고 서버 로그에는 적지 않는다.

`POST /auth/attempts` 본문:

| 칸 | 뜻 |
|---|---|
| `attempt_id` | UUID(표준 꼴 8-4-4-4-12). 필수 |
| `at` | 시도 시각, ISO(예: `2026-11-22T03:00:00Z`). 필수 |
| `credential_ok` | 자격이 맞았는가(true·false). 필수 |
| `ip` | 1~64자. 필수 |
| `user_agent` | 200자 이하(빈 문자열 허용). 필수 |
| `replayed` | true·false. **칸이 없을 때만** 기본 false — 칸이 있으면 null 이어도 422. true 는 관측 서버 장애 동안 어드민이 임시 제한으로 판정한 시도를 나중에 보내는 것 — **기록만 하고 잠금 계산에 넣지 않는다** |
| `outcome` | `replayed:true` 면 **필수** — 어드민이 실제 내린 판정 `success` · `failure` · `locked`(글자 그대로) 중 하나, 그 값을 그대로 기록한다. `replayed:false`(또는 빠짐)에 이 칸이 **있으면 값이 null 이어도** 422 |

응답 `data`: `{allowed, outcome("success"|"failure"|"locked"), locked_until(ISO 또는 null), lock_started, failures}`. `lock_started` 는 잠금으로 바뀐 그 한 번만 true(알림을 보낼 때). `replayed:true` 의 응답은 `allowed:false` · 보낸 `outcome` · 지금의 잠금 끝과 횟수 · `lock_started:false` 다(로그인 허용의 답이 아니다).

`POST /auth/attempts/rejected` 본문: `{attempt_id, at, reason, ip, user_agent}` — `reason` 은 `rate_limited` · `origin_rejected` · `malformed` 중 하나(그 밖은 422 — `not_configured` 같은 이유는 없다), 기록의 `outcome` 이 그 낱말이다. 기록만 하고 잠금 계산은 없다. 응답 `data`: `{recorded: true}`.

`GET /auth/attempts?from=&to=&before=&limit=`: 기간은 ISO, 최대 90일 — 둘 다 없으면 지금까지 90일, `from` 만 있으면 지금까지, `to` 만 있으면 그 앞 90일. `limit` 1~200, 기본 50. 이어 보기는 `/logs` 와 같다(응답의 `next_before` 를 `before` 로, `from`·`to` 는 앞 응답 것을 그대로; `before` 만 넘기면 422). 응답 `data`: `{attempts: [{id, at, outcome, ip, user_agent, replayed}], next_before, from, to}` — `outcome` 은 위 여섯 낱말 중 하나.

`GET /auth/lock`: 응답 `data`: `{locked, locked_until(ISO 또는 null), failures}` — 화면 표시용이고 판정에는 쓰지 않는다(판정은 `POST /auth/attempts` 한 번으로 끝난다). 잠금 끝이 지났으면 `failures` 는 0 으로 보인다.

오류(네 경로 공통 — 모두 `{success: false, error: {code}}` 봉투):

| HTTP | `code` | 언제 |
|---|---|---|
| 401 | `unauthorized` | 비밀값이 없거나 다르다(서버의 `OBSERVE_SECRET` 이 비면 전부). **본문 해석보다 먼저** 본다 — 본문이 어떤 꼴이든 401 |
| 422 | `invalid_request` | 본문·조건의 꼴이 틀리다(`detail` 에 이유). 칸은 있는데 값이 null 인 것도 꼴이 틀린 것이다(필수 칸 · `replayed` · `replayed:false` 의 `outcome`). 거절 기록 경로는 계약에 없는 칸을 무시한다. 두 POST 경로에서는 해석할 수 없는 본문도 여기다 — Content-Type 이 JSON 이 아니거나 없음 · 본문 없음 · 빈 글자 · 깨진 JSON · JSON 뒤의 군더더기 · 객체가 아닌 JSON(배열·글자·null) · 본문 16KiB 초과. 프레임워크의 400·415 는 나가지 않는다. 저장소에 닿지 않는다(기록 없음) |
| 503 | `query_failed` | DB 오류 · 연결 불가 · 시간 초과(예외 문구는 응답에 넣지 않고 서버 로그에만) |

| 설정(환경변수) | 기본 | 뜻 |
|---|---|---|
| `OBSERVE_AUTH_FAILURE_LIMIT` | `5` | 잠금까지의 연속 실패 횟수 |
| `OBSERVE_AUTH_LOCK_DURATION` | `15m` | 잠금 시간 |
| `OBSERVE_AUTH_RETENTION` | `90d` | 시도 기록 보관 기간 |

## 자원 지표

서버마다 도는 ci-cd agent 가 30초마다 그 서버의 자원 값(서버 전체 · 이 프로젝트의 컨테이너 · GPU)을 한 묶음으로 Redis 스트림 `metrics` 에 넣는다(로그와 같은 Redis, 스트림만 다르다). 관측 서버가 그것을 받아 원본 7일 · 5분 평균 30일 보관하고, 30초마다 임계값을 판정해 넘거나 풀리면 알림 서버로 알리고(아래 "임계값과 알림"), 어드민이 조회한다.

```
ci-cd agent(서버마다, 30초) ── XADD metrics ──▶ Redis ──▶ 관측 서버(지표 전용 수집 스레드) ──▶ PostgreSQL
                                                                 ├─ 임계값 판정(30초) ── POST /events ──▶ 알림 서버
                                                                 └─ GET /metrics/latest · /metrics/series · /metrics/containers ◀── 어드민
```

### 받는 스트림 항목

칸: `kind`(`resource`) · `v`(`1`) · `server`(agent 의 `SERVER_NAME`) · `at`(측정 시각, epoch 밀리초 — agent 의 시계) · `body`(JSON, 64KB 이하 — `interval_ms` · `host` · `containers` · `gpus` · `missing`).

- **저장한 뒤에만 확인(ack)한다** — 로그와 같은 규칙. 같은 항목(스트림 ID)을 두 번 받아도 한 번만 저장된다.
- 측정 시각과 수신 시각(관측 서버의 시계)을 둘 다 저장한다. 그래프와 5분 평균은 측정 시각, 헬스의 서버별 마지막 수신은 수신 시각으로 본다.
- 값이 `null` 인 것(첫 수집 · 누적 값이 줄어든 경우 등 agent 가 차이를 낼 수 없었던 값)은 `null` 로 저장하고 평균에 넣지 않는다. 읽지 못한 갈래(`missing`)는 빈 채로 저장한다.
- `kind` 가 `resource` 가 아닌 항목은 다른 종류다 — 저장하지 않고 건너뛰어 확인한다(건수는 헬스의 `skipped_other_kind`). 같은 길을 다른 종류가 쓸 수 있게 둔 것이다.
- **버리는 항목** — 그 항목만 버리고(같은 묶음의 다른 항목은 저장한다) 이유와 항목 ID 를 서버 로그에 남기고 이유별로 센 뒤 확인한다(다시 받지 않는다). 헬스의 `metrics.dropped` 에 이유 글자 그대로 나온다:

| 이유 | 언제 |
|---|---|
| `invalid_id` | 스트림 ID 가 `<밀리초>-<순번>` 꼴이 아니다 |
| `trimmed` | 칸이 하나도 없다 — 확인하지 못한 채 스트림 상한에 밀려났다 |
| `missing_field` | `kind` · `v` · `server` · `at` · `body` 중 빠진 칸(빈 글자 포함) |
| `unsupported_version` | `v` 가 `1` 이 아니다 |
| `invalid_server` | 서버 이름이 `a-z0-9._-`(첫 글자는 영숫자, 63자 이하) 꼴이 아니다 |
| `invalid_at` | `at` 이 양의 epoch 밀리초가 아니다 |
| `clock_ahead` | 측정 시각이 수신 시각보다 2분 넘게 앞선다(그 서버의 시계가 빠름 — 미래 값이 "최신"을 차지하지 않게) |
| `too_old` | 측정 시각이 수신 시각보다 6일 넘게 뒤다(원본 보관 7일과 하루의 여유 — 원본이 이미 지워진 구간의 5분 평균이 늦은 한 건으로 덮어써지지 않게). 정확히 6일은 받는다 |
| `body_too_large` | `body` 가 64KB(UTF-8 바이트)를 넘는다 |
| `body_not_json` | `body` 가 JSON 이 아니다 — 맨 `NaN` · `Infinity`, 객체 뒤에 더 붙은 글자 포함 |
| `invalid_body` | 꼴이 다르다 — 객체가 아님, 갈래가 객체·배열이 아님, 컨테이너 이름·ID 꼴, GPU 번호(0~999 정수) 등 |
| `invalid_value` | 값이 숫자·`null` 이 아니거나(글자 `"3"` · `true` 포함) 범위 밖 — 모든 값은 0 이상, 서버 CPU·GPU 사용률은 100 이하, GPU 온도는 −50~150, 초당 바이트(`*_bps`) 1e13 이하, 부하(`load1`) 1e6 이하, 컨테이너 CPU 1e6 이하(코어 하나 = 100 이라 100 은 넘을 수 있다), 바이트 수·재시작 횟수·`interval_ms` 는 정수(바이트 수에는 따로 상한이 없다 — 정수 칸의 범위가 상한). 유한하다는 것만으로는 받지 않는다 |

- 저장(DB)이 실패하면 확인하지 않는다 — 다음 바퀴에 다시 받아 저장한다. 버린 항목은 DB 와 무관해 그대로 확인한다.
- 지표 수집은 로그 수집과 **다른 스레드**(`observe-metric-collector`)와 다른 연결 풀로 돈다 — 지표 스트림에 무엇이 쌓이든, 지표 쪽이 실패하든 로그 수집은 그대로다.

### 보관

- **5분 평균**: 1분마다(`METRICS_ROLLUP_INTERVAL_MS`) 끝난 5분 구간(UTC 정각 5분 경계)마다 값별 평균 · 최댓값 · 표본 수를 만든다. 컨테이너는 자리 이름(`-blue` · `-green`)을 뗀 이름으로 묶고, 같은 항목에 두 자리가 함께 있으면 더한 것이 표본 하나다(전환 중의 실제 사용량). 평균을 만든 뒤 그 구간에 원본이 늦게 들어오면 다음 바퀴에 그 구간을 다시 계산해 덮어쓴다 — 여러 번 돌려도 결과가 같다.
- **구간마다 따로**: 한 (서버, 5분 구간)은 한 트랜잭션에서 계산하고 그 평균 값과 "여기까지 계산함" 표시를 함께 확정한다 — 한 구간의 계산 실패가 다른 구간의 결과를 되돌리지 않고, 실패한 구간에는 표시가 남지 않아 다음 바퀴에 다시 해 본다(실패 수는 헬스의 `metrics.last_rollup_failed_buckets`). 한 바퀴는 **가장 오래 시도하지 않은 구간부터 200 구간**까지(시도한 적 없는 구간이 맨 앞 — 계속 실패하는 구간이 앞자리를 차지해 뒤 구간을 막지 않는다. 시도한 시각은 `metric_rollup_attempt` 에 따로 남긴다). 밀린 뒤에도 저장을 오래 기다리게 하지 않는다 — 원본 삭제는 평균이 확인된 구간만이라 밀려도 잃지 않는다.
- **원본 7일**: 매일 03:40(서울)에 로그 삭제와 같은 예약 실행에서 지운다. **그 5분 구간의 평균이 원본을 모두 담은 뒤에 계산된 구간만** 지운다 — 평균이 없거나 평균 뒤에 원본이 더 들어온 구간은 남겨 두고 다음 날 다시 본다. 구간을 걸친 원본은 구간째 다음 날 지운다.
- **5분 평균 30일**: 구간 시작이 30일 지난 것을 같은 예약에서 지운다.
- 지표 삭제 · 로그 삭제 · 로그인 시도 삭제는 서로 따로 돈다 — 한쪽이 실패해도 나머지는 돈다. 결과는 헬스의 `metrics.last_retention_*` 와 서버 로그에.
- **표**: `metric_sample`(스트림 항목 하나 = 한 행) · `metric_container` · `metric_gpu`(항목 안의 컨테이너 · GPU) · `metric_rollup_bucket`(5분 구간의 계산 기록) · `metric_rollup`(5분 평균) · `metric_server_receipt`(서버별 마지막 수신 시각 — 원본과 따로 두어 원본이 7일 뒤 지워져도 남는다. 지표 저장과 같은 트랜잭션에서, **실제로 새로 저장된 항목만**(다시 전달된 항목은 치지 않는다) 큰 쪽으로 바뀐다. 표를 만들 때 원본에서 한 번 채운다 — 서버 이름순) · `metric_rollup_attempt`(5분 구간을 마지막으로 계산해 보려 한 시각 — 차례용). 표 만들기는 한 프로세스 안에서 한 줄로 선다(수집 스레드와 뜬 직후 미리 만들기가 겹쳐 첫 수집이 실패하지 않게). 기존 표는 건드리지 않는다. 뜬 직후 만들고, 그때 DB 가 없었으면 첫 수집 바퀴 · 첫 조회가 만든다(`CREATE … IF NOT EXISTS`).
- 연결 풀은 지표 전용(수집 2 · 조회 2 · 헬스의 상태 읽기 1 · 헬스의 선택 칸 읽기 1)이다. 판정이 읽는 표본은 판정 · 알림 풀로 읽는다(어드민 조회가 몰려도 판정이 기다리지 않게). 5분 평균은 구간마다 원본 저장을 잠시 기다리게 한다(계산하는 사이에 커밋된 원본을 놓치지 않게 — 읽기는 막지 않는다). 그동안 읽는 "지금까지의 저장 순번"은 원본 표를 훑지 않고 순번 발번기(`insert_order` 칸의 시퀀스)의 마지막 값이다 — 잠금을 쥔 동안에는 진행 중인 저장이 없으므로(저장은 표의 잠금을 얻은 뒤에야 번호를 받는다) 그 값은 저장된 모든 원본의 순번 이상이다. 되돌려진 저장이 번호를 써 버려 더 커도 뒤의 저장은 더 큰 번호를 받으므로 "다시 계산" · "삭제 보류"의 조건은 그대로 선다. 이 방식은 발번기가 번호를 하나씩 내준다(캐시 1 — 기본값)는 전제에 서므로, 구간마다 그 캐시도 함께 읽어 **1 이 아니거나 값이 비어 오면(못 찾음 · 아직 내준 번호 없음) 그 구간을 실패로** 처리한다(권한이 없어 읽지 못해도 실패). 평균도 "계산함" 표시도 남지 않으므로 원본은 지워지지 않고, 헬스의 `last_rollup_failure` 에 `InsertOrderSequenceUnusable` 로 드러난다 — 발번기를 손댔다면 `ALTER SEQUENCE … CACHE 1` 로 되돌린다.

### 조회 API

전부 GET, 전부 `X-Observe-Secret`(인증을 입력 검사보다 먼저 본다). 봉투와 오류는 `/logs` 와 같다 — 401 `unauthorized` · 422 `invalid_request`(`detail`) · 503 `query_failed` · 504 `query_timeout`.

| 경로 | 설명 |
|---|---|
| `GET /metrics/latest` | `{servers: [{server, at, received_at, age_ms, stale, host, containers, gpus, missing}], firing: [{server, rule, target, severity, since, last_value}]}` — `firing` 은 지금 넘어 있는 규칙(아래 "임계값과 알림") — 확정된(커밋된) 판정 상태를 지표 조회용 연결로 읽는다(판정 · 알림용 연결이 다 잡혀도 답한다). 이 길은 표를 만들지도 표 만들기 잠금을 쥐지도 않는다 — 판정 표가 아직 없으면 `firing` 은 빈 배열이다(표는 뜬 직후와 판정 · 보내기의 길이 만든다). `servers` 는 최근 24시간에 받은 서버마다 측정 시각이 가장 늦은 묶음. `at` 은 측정 시각, `age_ms` 는 수신 시각부터, `stale` 은 3분 초과. 서버 목록은 최근 24시간에 받은 서버 + `METRICS_EXPECTED_SERVERS` + 마지막 수신 기록(`metric_server_receipt`)이 있는 서버다 — 원본이 지워진 서버도 빠지지 않는다. 보일 값이 없는 서버는 "수신 없음"(`at` · `host` 가 null, `stale` true, 마지막 수신 기록이 있으면 `received_at` · `age_ms` 를 채우고 없으면 null). 이름순. 컨테이너에는 `series_name`(자리 이름을 뗀 이름)이 붙는다 |
| `GET /metrics/series?server=&scope=&name=&from=&to=` | 계열 하나의 점 — `{server, scope, name, from, to, step_ms, source, points: [{at, <값>, <값>_max, …}]}` |
| `GET /metrics/containers?server=&at=` | 그 서버에서 보인 컨테이너(자리 이름을 뗀 이름)와 마지막으로 보인 측정 시각 — `{server, from, to, containers: [{name, last_seen_at}]}`. `at` 이 없으면 최근 24시간, 있으면 그 시각 앞뒤 5분(연관 로그가 "그때 그 서비스가 있던 서버"를 고를 때) |

`GET /metrics/series` 의 조건:

| 이름 | 뜻 |
|---|---|
| `server` | 필수. 서버 이름(위 `invalid_server` 와 같은 꼴) |
| `scope` | 필수. `host`(값: `cpu_pct` · `load1` · `mem_total` · `mem_used` · `disk_total` · `disk_used` · `net_rx_bps` · `net_tx_bps` · `io_read_bps` · `io_write_bps`) · `container`(`cpu_pct` · `mem_used`) · `gpu`(`util_pct` · `mem_used` · `mem_total` · `temp_c`) |
| `name` | host 는 주지 않는다(주면 422). container 는 자리 이름을 뗀 이름(`front-app`), gpu 는 번호(`0`, 1~3자리) — 필수 |
| `from` · `to` | ISO 시각. `/logs` 와 같이 둘 다 없으면 최근 1시간. 최대 31일(정확히 31일은 받는다) |

- **간격**: `step_ms` = 기간 ÷ 500 을 30초 단위로 올린 것(최소 30초) — 점은 500 개를 넘지 않는다. 점의 `at` 은 `from` 에서 센 칸의 시작이다.
- **읽는 곳**: 측정 시각이 `지금 − 7일`(5분 단위로 내림)보다 앞선 부분은 5분 평균에서(그때는 간격 최소 5분), 나머지는 원본에서 읽는다. `source` 는 `raw` · `rollup` · `mixed`(경계를 걸친 요청).
- **5분 평균의 구간 경계**: 5분 평균에서 읽을 때는 요청 기간과 조금이라도 겹치는 구간을 읽고, 한 구간은 통째로 한 점이다 — 점의 시각은 구간의 시작이라 **첫 점이 `from` 보다 최대 5분 앞설 수 있다**(칸도 5분 경계에서 센다).
- 칸 안의 값은 평균과 최댓값(`_max`)이다. 5분 평균을 묶을 때는 **표본 수로 가중한 평균**과 최댓값의 최댓값을 쓴다. 값이 없는 칸은 점을 만들지 않는다(0 으로 채우지 않는다). 점 안에서 표본이 없는 값은 null.
- 컨테이너는 같은 시각에 두 자리가 있으면 합(CPU · 메모리)으로 본다.

### 헬스의 `metrics` 칸

`GET /health` 본문에 `metrics` 객체가 붙는다. **지표 쪽이 어떻게 실패해도 로그 수집의 `status` 와 HTTP 상태는 바뀌지 않는다** — 지표의 상태는 `metrics.status` 에만 나온다.

| 칸 | 뜻 |
|---|---|
| `status` | 로그와 같은 낱말 · 같은 판정 순서(`unreachable` → `failing` → `ok` → `starting` → `stalled`), 지표 수집 바퀴 · 지표 스트림 · 지표 표 기준 |
| `last_loop_at` · `last_stored_at` · `stored_since_start` · `unacknowledged` · `lag` · `failure` | 로그의 같은 칸과 같은 뜻(지표 스트림·지표 표 기준). `last_stored_at` 은 가장 최근 항목의 수신 시각 |
| `dropped` · `dropped_total` · `skipped_other_kind` | 이 프로세스가 뜬 뒤 버린 항목 수(이유별 — 모든 이유가 0 부터) · 그 합 · 다른 종류라 건너뛴 수 |
| `last_received_at` | 서버 이름 → 마지막 수신 시각(`metric_server_receipt` — 원본이 지워져도 남는다. 읽지 못하면 빈 객체) |
| `clock_behind` | 시계가 뒤처진 서버 이름(이름순) — 가장 최근 항목의 수신 − 측정이 1분을 넘는 서버. 처음 그렇게 된 서버 · 풀린 서버는 서버 로그에 1줄씩. 그런 서버는 디스크 · 메모리 · CPU 판정이 미뤄질 수 있다. 읽지 못하면 빈 목록 |
| `silent_judging` | 지금 끊김 규칙을 판정하는가(아래 "임계값과 알림" — 뜬 직후에는 false) |
| `last_rollup_at` · `last_rollup_failure` | 5분 평균의 마지막 **성공** 시각 — 실패한 구간이 하나도 없는 바퀴만 성공이다(일부만 실패해도 성공 시각은 그대로) · 마지막 바퀴의 실패 사유(바퀴가 통째로 실패하면 예외 종류, 일부 구간이 실패하면 `<실패 구간 수> buckets failed: <첫 실패의 종류>`, 성공이면 빈 문자열) |
| `last_rollup_failed_buckets` | 마지막 5분 평균 바퀴에서 계산에 실패한 구간 수(아직 안 돌았으면 -1) — 그 구간은 다음 바퀴에 다시 해 본다 |
| `last_retention_at` · `last_retention_raw_deleted` · `last_retention_rollup_deleted` · `last_retention_failure` | 마지막 지표 삭제의 시각 · 지운 원본 항목 수 · 지운 (서버, 5분 구간) 수 · 실패 종류(안 돌았거나 실패면 빈 문자열 · -1) |
| `last_judged_at` | 이 자리가 마지막으로 판정 잠금을 얻어 판정한 시각(없으면 빈 문자열 — 다른 자리가 판정 중이면 오래돼 보인다) |
| `last_judge_failure` · `last_dispatch_failure` | 마지막 바퀴의 판정 · 보내기 단계가 실패했으면 그 종류(성공이면 빈 문자열) |
| `undelivered_alerts` · `rejected_alerts` | 아직 보내지 못한 알림 수 · 알림 서버가 거절해 다시 보내지 않는 알림 수 — 판정 · 보내기 바퀴가 끝날 때 세어 둔 값(아직 못 셌거나 마지막에 읽지 못했으면 -1) |
| `skipped_unknown_rule_alerts` | 아직 보내지 못한 알림 가운데 이 판이 모르는 규칙 글자 · 등급 글자라 건너뛰는 줄의 수(`undelivered_alerts` 에도 든다) — 같은 때 세어 둔 값(아직 못 셌거나 읽지 못했으면 JSON 의 `null` — 다른 칸의 -1 과 다르다). 아래 "임계값과 알림"의 "모르는 규칙 글자의 줄" |

판정 · 보내기의 실패도 `metrics.status` 와 로그 수집의 `status` 를 바꾸지 않는다 — 위 칸에만 나온다. 헬스는 판정 · 보내기용 연결을 쓰지 않고 표를 만들지도 않는다. 지표 칸의 읽기는 로그 쪽 읽기와 **동시에** 시작한다 — 헬스 전체의 최악 응답 시간은 이번 기능 전과 같다(둘의 합이 아니라 긴 쪽). 지표 칸이 묻는 것(Redis 의 미확인 · 밀린 건수, DB 의 마지막 저장 시각 · 서버별 수신 시각 · 최근 항목)은 한꺼번에 묻고 합쳐 2초까지만 기다린다 — 미확인 건수나 마지막 저장 시각을 그 안에 못 얻으면 `metrics.status` 는 `unreachable`(`failure` 에 `… TimeoutException`), 그 밖의 칸은 "읽지 못함"(-1 · 빈 값)이다. 상태를 정하는 읽기(마지막 저장 시각)와 선택 칸의 읽기는 **연결 풀이 다르고** 둘 다 연결을 얻는 데에도 2초 마감이 있다 — 선택 칸의 표가 막힌 채 헬스가 되풀이돼도 `metrics.status` 는 그대로다. 로그의 `status` 와 HTTP 판정은 그대로다.

### 설정

| 설정(환경변수) | 기본 | 뜻 |
|---|---|---|
| `METRICS_STREAM` | `metrics` | 받는 스트림(agent 의 같은 이름 설정과 같은 값) |
| `METRICS_GROUP` | `observe` | 소비자 그룹 이름 |
| `METRICS_EXPECTED_SERVERS` | (없음) | 지표를 보내야 하는 서버 이름, 쉼표로 — 한 번도 보내지 않은 서버도 `GET /metrics/latest` 에 "수신 없음"으로 |
| `METRICS_ROLLUP_INTERVAL_MS` | `60000` | 5분 평균 바퀴의 간격(밀리초) |
| `METRICS_JUDGE_INTERVAL_MS` | `30000` | 임계값 판정 바퀴의 간격(밀리초) |
| `ALERT_DISPATCH_INTERVAL_MS` | `30000` | 알림 보내기 바퀴의 간격(밀리초) — 판정과 따로 돈다 |
| `ALERT_EVENTS_URL` | (없음) | 알림 서버의 `/events` 까지 적은 주소(ci-cd 와 같은 이름). 비면 보내지 않고 못 보낸 알림으로 남긴다(`not_configured`) |
| `ALERT_SECRET` | (없음) | 알림 서버의 `X-Alert-Secret` 값 — 비밀값, 서버 `.env` 에만 |
| `ALERT_TIMEOUT_MS` | `3000` | 알림 하나를 보내는 기한(연결과 응답 전체) |
| `METRICS_ALERT_*` | 위 규칙표 | 임계값 — 아래 "임계값과 알림"의 키 |

### 임계값과 알림

30초마다(`METRICS_JUDGE_INTERVAL_MS`) 저장된 최근 10분의 표본으로 **지금의 상태**만 판정한다. 밀린 항목이 한꺼번에 들어와도, 관측 서버가 꺼져 있다 돌아와도 지난 구간의 넘음 · 해소를 되짚어 알리지 않는다. 지속 시간은 측정 시각으로, 끊김은 수신 시각으로 잰다.

| 규칙(`rule`) | 넘음(알림) | 해소 | 알림 `severity` |
|---|---|---|---|
| `disk_warning` | 루트 디스크 사용률이 최근 2분 안의 가장 최근 2회 모두 85% 이상 | 2회 모두 82% 아래 | warning |
| `disk_critical` | 2회 모두 95% 이상 — 정상에서 곧바로도, 경고에서 오를 때도 **위험 알림 하나만**(경고를 따로 보내지도, "경고 해소"를 함께 보내지도 않는다) | 2회 모두 92% 아래 → 경고로 내려옴(`disk_critical:resolved`, "위험 해소, 경고 유지", warning) · 82% 아래면 곧바로 정상(`disk_critical:resolved` 하나, info) | critical |
| `memory` | 메모리 사용률(`mem_used ÷ mem_total`)이 5분 동안 모두 90% 이상 | 5분 동안 모두 85% 아래 | warning |
| `cpu` | 서버 CPU 사용률이 10분 동안 모두 90% 이상 | 5분 동안 모두 80% 아래 | warning |
| `restarts` | 한 컨테이너(ID 별)의 재시작 횟수가 10분 안에 3 이상 늘어남 | 10분 동안 늘지 않음 · 그 컨테이너가 사라지고 서버는 계속 보냄(새 컨테이너로 바뀜) | warning |
| `silent` | 서버의 마지막 수신이 3분 넘게 전이거나 한 번도 없음(기대 서버 + 한 번이라도 보낸 서버 — 원본이 지워진 서버도 마지막 수신 시각이 남아 계속 판정한다) — 정확히 3분은 아니다 | 다시 들어옴 | warning |

- **결측은 미룬다**: 창 안의 값(null 제외)이 기대 수(창 ÷ 30초)의 절반에 못 미치거나, 가장 오래된 값이 창의 시작에서 30초 안에 있지 않으면(창을 덮지 못함 — 막 들어오기 시작한 서버) 그 규칙은 상태를 그대로 둔다. 결측은 넘음도 해소도 아니다. 디스크는 최근 2분 안에 값이 2개 있어야 한다. 재시작의 넘음은 창을 덮지 않아도 되지만 절반 규칙은 따른다.
- **재시작은 컨테이너 ID 별**이다 — 배포로 컨테이너가 새로 만들어지면 새 기준이라 배포가 재시작으로 세지지 않는다.
- **알림 key**: `resource:<서버>:<규칙>[:<컨테이너 이름>]`, 해소는 그 뒤에 `:resolved`(알림 서버가 같은 key 를 10분에 한 번만 보내므로 넘음과 해소의 key 를 나눈다). 디스크는 단계마다 규칙이 다르다(`disk_warning` · `disk_critical` — 경고 뒤에 오는 위험 알림이 억제되지 않게). key 는 120자를 넘지 않는다(길면 앞부분과 해시로 줄인다). `kind` 는 `resource_disk` · `resource_memory` · `resource_cpu` · `resource_restarts` · `resource_silent`, `source` 는 `observe`.
- **로그 알림도 같은 보낼 표를 쓴다**: 규칙 글자 `log_silent`(서비스 하나의 무수신 — key `log:<서비스>:silent`, 서버는 그 서비스가 사는 서버 · key 에는 서버가 들지 않는다) · `log_silent_all`(전체 스트림 한 줄의 무수신 — key `log-all:silent`, 서버 `*`), `kind` 는 둘 다 `log_silent`. 넘음 warning · 해소 info(key 뒤에 `:resolved`), key 120자 이하(길면 앞부분과 해시로 줄인다). 서비스 이름을 다듬어 글자가 바뀌었거나 이름에 `~` 가 들면 key 는 `log:<다듬은 이름>~<원래 이름의 SHA-256 앞 12자>:silent` — 숨은 문자만 다른 이름들이나 `*` 라는 서비스가 다른 알림과 한 key 가 되어 보낼 표에서 조용히 버려지지 않게. title · detail · key 는 글자의 경계에서 자른다(200 · 1500 · 120 — UTF-16 단위). 표의 칸은 자원 알림과 같다(칸을 더하지 않는다) — 보내기 · 다시 시도 · 대상의 차례 · 한 바퀴 50건 · 30초도 같고 종류마다의 몫은 없다(로그 알림이 한 바퀴를 다 차지하면 자원 알림은 다음 바퀴에 나간다). 알림의 글에는 서비스 · 서버 이름 · 마지막으로 받은 시각 · 기준만 들고 로그의 글은 싣지 않는다 — 이름의 제어 문자 · 줄 · 문단 구분 문자 · 서식 문자는 `?` 로 바뀐다. **배포 순서**: 로그 규칙 글자를 모르는 판(02a 전)은 그 줄을 읽다 예외로 보내기 바퀴 전체가 멈추므로, "모르는 줄을 건너뛰고 센다"를 넣은 판(02a)을 먼저 올린 뒤에 로그 알림을 쓰는 판을 올린다 — 그래야 두 판이 함께 도는 동안에도 옛 판이 새 줄을 건너뛰고 자원 알림을 보낸다.
- **재시작의 해소**는 그 창에서 재시작이 늘지 않았거나, 그 컨테이너 ID 가 사라졌을 때다. "사라짐"은 컨테이너 갈래가 온전한 표본(`missing` 에 `containers` · `containers_truncated` 가 없음)이 창의 절반 이상이고 **가장 최근 표본도 온전**하며 그 표본들에 그 ID 가 없을 때만이다 — 컨테이너를 읽지 못한 것은 넘음도 해소도 아니다(보류). 재시작 횟수의 증가량도 온전한 표본만으로 센다 — 잘린 표본의 값은 넘음 · 해소 어느 쪽도 좌우하지 않는다. 해소 알림의 제목에 까닭이 붙는다("컨테이너가 바뀜" · "재시작이 멈춤").
- **끊김 판정의 시작**: 이 프로세스가 뜬 지 3분(`METRICS_ALERT_SILENT_WARM_UP`)이 지나고, 뜬 뒤로 지표 수집이 한 번이라도 따라잡았을 때(읽을 것이 없는 바퀴가 한 번 있었음)부터 끊김을 판정한다. 따라잡지 못한 채 15분(`METRICS_ALERT_SILENT_GIVE_UP_AFTER`)이 지나면 그때부터는 판정한다. 그 전에는 끊김 규칙 전체를 보류한다 — 새로 울리지도, 울리던 것을 해소하지도, 다시 알리지도 않는다(관측 서버가 꺼져 있던 동안을 "모든 서버가 끊겼다"로 알리지 않게). 다른 규칙은 그대로 판정한다. 헬스의 `metrics.silent_judging` 에 보인다.
- **알리는 때**: 넘을 때 1회, 해소될 때 1회(`info` — 디스크 위험에서 경고로 내려올 때만 `warning`), 넘어 있는 채로 마지막 알림에서 1시간이 지나면 같은 key 로 다시 1회(처음 넘은 시각은 그대로). `:resolved` 는 내려가거나 정상으로 돌아올 때만 간다.
- **판정 상태는 PostgreSQL 에**(`resource_rule_state`) — 관측 서버가 재시작하거나 자리를 바꿔도 같은 알림을 다시 만들지 않고 해소를 놓치지 않는다. 상태 변경과 보낼 알림(`resource_alert_outbox` — 알림마다 고유 키)은 **한 트랜잭션**에 기록한다.
- **한 자리만 판정 · 한 자리만 보낸다**: 판정과 보내기는 각각 PostgreSQL 의 권고 잠금(기다리지 않고 시도)을 얻은 자리만 돈다. 표 잠금이 아니라서 지표 저장 · 조회 · 5분 평균 · 로그와 서로 막지 않는다. 보내기 잠금은 보내는 한 바퀴 동안(최대 30초) 쥔다 — 그동안 막히는 것은 다른 자리의 보내기뿐이다.
- **보내기는 판정과 따로 돈다**(`ALERT_DISPATCH_INTERVAL_MS`): 예약 실행기는 스레드가 넷이라 알림 서버가 느려도 30초 판정 · 5분 평균 · 삭제는 밀리지 않는다. 판정 잠금을 얻은 자리만 표본을 읽는다.
- **보내기의 결과**: 2xx = 전달(표에서 지운다). 5xx · 429 · 401 · 403 · 연결 실패 · 기한 초과 · 주소 없음(`not_configured`) = 남겨 두고 다음 바퀴에 다시(헬스의 `undelivered_alerts`; 401 · 403 은 비밀값을 고치면 풀린다). 그 밖의 4xx = 거절 — 다시 보내지 않고 `rejected_alerts` 로 센다. 실패 사유는 HTTP 코드나 예외 종류 이름만 남긴다(주소 · 비밀값은 로그 · 헬스에 나오지 않는다).
- **보내기는 대상을 돌아가며 한 건씩**: 보낼 알림은 대상(서버 · 규칙의 묶음 · 컨테이너 — **디스크 경고와 위험은 한 묶음**이라 도착 순서가 바뀌지 않는다)마다 줄을 선다. 한 바퀴는 차례대로 대상마다 머리 한 건씩을 보내고, 한 바퀴를 다 돌면 처음 대상으로 돌아가 다음 건을 보낸다 — 한 대상에 알림이 아무리 많이 밀려 있어도 다른 대상이 그 뒤에서 기다리지 않는다. 같은 대상 안의 순서는 그대로다. "다시 시도"가 난 대상은 그 바퀴에서 빠진다 — 한 대상이 아무리 오래 · 많이 실패해도 다른 대상의 머리는 매 바퀴 읽힌다. 대상의 차례는 **가장 오래 시도하지 않은 대상부터**(한 번도 시도하지 않은 대상이 맨 앞, 방금 시도한 대상은 맨 뒤 — 시간이 모자라 차례가 오지 않은 대상은 다음 바퀴에 먼저 간다 — 대상마다 마지막 시도 시각을 `resource_alert_target` 에 남긴다. 알림 줄이 지워져도 남는다). 지금까지 쓴 시간에 한 건의 기한(`ALERT_TIMEOUT_MS`, 3초)을 더해 30초를 넘게 되면 새로 보내지 않는다 — 바퀴 전체가 30초를 넘지 않는다. 한 바퀴에 최대 50건.
- **모르는 규칙 글자 · 등급 글자의 줄은 건너뛰고 센다**: 보낼 표에 이 판이 모르는 규칙 글자(`rule` — 위 표와 로그 규칙의 글자와 정확히 같지 않은 것, 대소문자만 다른 글자 · 빈 글자 포함)나 모르는 등급 글자(`severity` — `info` · `warning` · `critical` 이 아닌 것)의 줄이 있으면 보내지도 표시하지도 지우지도 않고 건너뛴다 — 판을 갈아 끼우는 동안 새 판이 쓴 줄을 옛 판이 읽어도 보내기가 멈추지 않게. 그 줄은 한 바퀴의 50건 · 30초 · 대상의 차례를 먹지 않고, 남아 있다가 그 규칙을 아는 판이 처음 보내는 줄처럼 보낸다. 건너뛴 수는 보내기 바퀴가 끝날 때 보내기 잠금을 놓은 뒤에 한 번 센 건수(보낼 표의 건수 세기 하나 — 시간 상한 2초)로 세고, 그 한 결과가 헬스의 `skipped_unknown_rule_alerts`(규칙 글자별 수의 합)와 경고 로그 한 줄(`alerts skipped unknown rule lines … log_errors=4, … · total 7` — 등급 글자를 모르는 줄도 그 줄의 규칙 글자로 센다 — 잠금을 얻지 못한 바퀴도 센다. 규칙 글자는 제어 문자 · 줄 · 문단 구분 문자 · 서식 문자를 `?` 로 바꾸고 글자마다 64 코드포인트까지, 규칙 글자 순으로 앞의 10가지만 찍고 나머지는 ` (+N more)`, `total` 은 전부의 합)에 함께 드러난다. 세지 못하면 그 바퀴의 보내기는 그대로이고, 헬스는 `null`, 경고 로그는 한 줄(`alerts could not count unknown rule lines: <예외 종류>`)이다. 판정 바퀴의 세기는 헬스만 바꾼다.
- 판정 · 보내기의 어떤 실패도 지표 수집 · 로그 수집 · 5분 평균 · 헬스의 기존 판정을 막거나 바꾸지 않는다.

임계값 설정 — 설정 키 `observe.metrics.alert.<이름>` 또는 환경 변수 `METRICS_ALERT_<이름>`(비우면 기본값, 말이 안 되는 값이면 뜰 때 실패한다. 시간은 `5m` · `1h` 꼴):

| 환경 변수 | 기본 | 뜻 |
|---|---|---|
| `METRICS_ALERT_DISK_WARNING_PCT` · `METRICS_ALERT_DISK_CRITICAL_PCT` | `85` · `95` | 디스크 경고 · 위험 기준(%) |
| `METRICS_ALERT_DISK_RESOLVE_MARGIN_PCT` | `3` | 디스크 해소의 되돌림 폭(%p) |
| `METRICS_ALERT_DISK_CONSECUTIVE` · `METRICS_ALERT_DISK_RECENT` | `2` · `2m` | 디스크의 연속 횟수 · 그 값을 찾는 최근 구간 |
| `METRICS_ALERT_MEMORY_PCT` · `METRICS_ALERT_MEMORY_FOR` | `90` · `5m` | 메모리 넘음 기준 · 지속 |
| `METRICS_ALERT_MEMORY_RESOLVE_PCT` · `METRICS_ALERT_MEMORY_RESOLVE_FOR` | `85` · `5m` | 메모리 해소 기준 · 지속 |
| `METRICS_ALERT_CPU_PCT` · `METRICS_ALERT_CPU_FOR` | `90` · `10m` | CPU 넘음 기준 · 지속 |
| `METRICS_ALERT_CPU_RESOLVE_PCT` · `METRICS_ALERT_CPU_RESOLVE_FOR` | `80` · `5m` | CPU 해소 기준 · 지속 |
| `METRICS_ALERT_RESTART_INCREASE` · `METRICS_ALERT_RESTART_WINDOW` | `3` · `10m` | 재시작 증가 수 · 창 |
| `METRICS_ALERT_SILENT_AFTER` | `3m` | 끊김 기준 |
| `METRICS_ALERT_REMIND_EVERY` | `1h` | 다시 알림 간격 |
| `METRICS_ALERT_SAMPLE_INTERVAL` | `30s` | agent 의 수집 간격 — 창의 기대 표본 수와 "창을 덮었는가"의 기준 |
| `METRICS_ALERT_SILENT_WARM_UP` | `3m` | 끊김 판정을 시작하기까지 — 이 프로세스가 뜬 지 이만큼 지나고 지표 수집이 한 번 따라잡은 뒤부터 |
| `METRICS_ALERT_SILENT_GIVE_UP_AFTER` | `15m` | 따라잡지 못해도 뜬 지 이만큼 지나면 끊김을 판정한다(warm-up 보다 짧으면 뜰 때 실패) |

### 로그 무수신 알림

헬스는 정상인데 로그만 끊긴 것을 안다 — 서비스마다(`log_silent`)와 전체 스트림 한 줄(`log_silent_all`)의 마지막 로그(Redis 가 받은 시각)가 기준보다 **엄격히** 오래되면(받은 기록이 없어도) 알린다. 판정은 자원 판정과 같은 주기(`METRICS_JUDGE_INTERVAL_MS`)로 돌고, 알림은 자원 알림과 같은 보낼 표 · 같은 보내기로 간다(위 "로그 알림도 같은 보낼 표를 쓴다").

- **설정**: `LOG_ALERT_SERVICES=<서비스>@<사는 서버>=<기준(분)>,…`(예: `backend@ci-agent-158=30,front@ci-agent-9=45`) · `LOG_ALERT_ALL_THRESHOLD_MINUTES=<분>`(전체 한 줄 — 비면 감시하지 않는다). 기준은 1~360분(6시간을 넘는 서비스는 감시하지 않고 전체 한 줄에 맡긴다). 둘 다 비거나 받아들인 줄이 없으면 판정이 꺼진다(기동 로그에 `log alerts disabled`). 감시하는 것과 받아들이지 않은 줄(다듬은 글자와 까닭)은 기동 로그에 한 줄씩 남는다. 받아들이지 않은 줄만 빠지고 다른 서비스는 감시한다. 이름 · 서버 · 기준의 조각은 앞뒤 공백을 다듬는다(중복도 다듬은 이름으로 본다). 사는 서버가 자원 판정의 기대 서버 목록(`METRICS_EXPECTED_SERVERS`)에 없으면 받아들이되 기동 로그에 경고 한 줄(`server_not_expected` — 그 서버의 끊김 억제가 듣지 않을 수 있다. 목록이 비면 대조하지 않는다).

| 까닭 | 뜻 |
|---|---|
| `name_reserved` | 서비스 이름이 `*`(전체 한 줄의 자리) |
| `name_empty` | 서비스 이름이 비었다 |
| `name_hidden_characters` | 서비스 이름에 제어 문자 · 줄 · 문단 구분 문자 · 서식 문자 · 짝 없는 서로게이트가 들었다 |
| `name_invalid` | 서비스 이름이 수집이 받는 꼴(`[a-z0-9-]{1,40}` — 대문자 · 공백 · 밑줄 · 점 · 한글은 안 된다)이 아니다 — 그런 이름의 로그는 오지 않아 늘 "받은 기록 없음"이 된다 |
| `name_duplicated` | 같은 서비스 이름이 두 번 나온다(서버가 달라도 — 그 이름의 줄 모두. 상태의 열쇠가 서비스라서) |
| `server_missing` · `server_invalid` | 사는 서버가 없다 · `*` 이거나 숨은 문자가 들었다 |
| `no_threshold` · `threshold_not_number` · `threshold_not_positive` · `threshold_over_6h` | 기준이 없다 · 정수가 아니다 · 0 이하 · 360분을 넘는다 |

- **알리는 때**: 넘을 때 1회(warning), 넘어 있는 채로 마지막 알림에서 1시간이 지나면 `(계속)` 1회, 다시 받으면 해소 1회(info — 알린 넘음만). 서비스 · 규칙마다 상태 하나(`log_rule_state` — 자원 판정의 상태 표와 따로, 판정 잠금도 따로라 서로 기다리지 않는다). 설정에서 뺀 서비스(또는 다른 자리만 감시하는 서비스) · 끈 전체 한 줄의 상태 줄은 지우지 않고 남는다 — 두 자리의 설정이 배포 중에 달라도 서로의 상태를 지워 같은 넘음을 다시 알리지 않게. 다시 설정에 넣으면 그 앞 상태에서 이어 간다(넘어 있었고 지금 받고 있으면 해소 한 번).
- **보류**: 이 프로세스가 뜬 지 3분(`LOG_ALERT_WARM_UP`)이 지나야 판정한다. 그 뒤에도 로그 수집이 **밀린 동안**(마지막으로 성공한 수집 바퀴가 한 바퀴의 상한만큼 읽었다 — 상한보다 적게 읽은 바퀴가 따라잡은 바퀴다) · 실패 중인 동안 · 마지막 수집 바퀴가 60초 넘게 오래된 동안은 보류한다 — 수집이 밀리거나 멈춘 동안의 공백은 로그가 없는 것이 아니다(DB 가 끊겼다 돌아온 첫 바퀴에 받고 있던 서비스를 넘음으로 알리지 않게). 자원 끊김과 달리 15분 뒤에 포기하고 판정하는 일은 없다(밀린 동안의 판정은 거짓 알림이다). 보류 중에는 새로 울리지도 해소하지도 다시 알리지도 않는다.
- **서버 끊김 때 억제**: 서비스가 사는 서버가 자원 끊김(`silent`)으로 넘어 있으면 그 서비스의 무수신을 알리지 않고 넘음만 남긴다(서버 끊김 알림 하나로 충분하게). 서버가 돌아온 뒤에도 조용하면 그때 처음 알린다. 알린 적 없는 넘음은 해소도 알리지 않는다. 전체 한 줄은 서버 끊김과 무관하다.
- **헬스의 `log_alerts` 칸**: `enabled` · `watched_services`(감시하는 서비스 수) · `watch_all`(전체 한 줄을 감시하는가) · `judging`(지금 판정하는가 — 보류면 false) · `last_judged_at`(이 자리가 마지막으로 잠금을 얻어 판정한 시각, 없으면 빈 문자열) · `last_failure`(마지막 바퀴가 실패했으면 그 예외 종류, 없으면 빈 문자열 — 문구는 싣지 않는다).
- **알려진 한계**: 수집이 영영 밀리면(상한만큼씩만 읽는 바퀴가 이어지면) 무수신 알림이 나가지 않는다 — 헬스의 `log_alerts.judging` 이 false 로 보인다. 판정은 설정에 적은 서비스만 본다(서비스 이름은 보내는 쪽의 글자라 새 이름은 설정에 더해야 감시한다). 마지막 로그는 Redis 가 받은 시각이라 보내는 쪽이 로그를 쌓아 두었다 한꺼번에 보내면 그 사이는 무수신으로 보일 수 있다. 판정의 조회는 2초에서 끊긴다(그 바퀴는 `last_failure` 에 남고 다음 바퀴에 다시 돈다). 오래 묵은 넘음 · 해소 알림을 버리는 규칙은 없다.

### 로그 글자 규칙

설정한 (서비스 · 글자)가 든 로그 줄이 있으면 알림 서버로 사건을 보낸다(쓰임: backend 의 `claude run: result=limit_reached` 같은 줄). 판정은 로그를 받는 길이 아니라 **무수신 판정과 같은 주기의 바퀴에서** 저장된 로그 표를 읽어 한다 — 두 자리가 로그 줄을 나눠 받아도 사건은 한 건이고(판정 잠금 · 상태 표 `log_pattern_state` — 무수신과 다른 잠금), 판정의 실패가 수집에 닿지 않는다. 무수신 판정과 따로 돈다(한쪽이 꺼지거나 실패해도 다른 쪽은 돈다). 판정의 문도 무수신과 같다(뜬 뒤 warm-up · 로그 수집이 밀리거나 멈춘 동안은 보류).

- **설정**: `LOG_ALERT_PATTERNS=<이름>|<서비스>|<세기>|<글자>;…` — 규칙은 `;` 로(앞뒤 빈칸 · 빈 규칙은 무시), 글자는 셋째 `|` 뒤의 전부(`|` · `=` · `,` · 안의 빈칸을 품는다 — 조각마다 앞뒤 빈칸만 깎는다). 이름 `[a-z0-9_]{1,40}` · 서비스는 수집이 받는 꼴(`[a-z0-9-]{1,40}`) · 세기 `info` · `warning` · `critical`(대소문자 구분) · 글자 1~120자 · 규칙 20개까지. 틀린 규칙은 그 규칙만 버리고 기동 로그에 다듬은 글자와 까닭을 한 줄 남긴다(쓰는 규칙의 줄에는 글자를 싣지 않는다). 비면 꺼짐(`log pattern alerts disabled — no patterns`). 예(이 저장소의 쓰임 — `.env.example` 에 그대로):
  `claude_escalated|backend|info|claude run: result=answered auto=true;claude_limit|backend|warning|claude run: result=limit_reached;claude_unavailable|backend|warning|claude run: result=unavailable;claude_timeout|backend|warning|claude run: result=timeout`

| 까닭 | 뜻 |
|---|---|
| `fields_missing` | 조각(세로줄로 가른 것)이 넷이 안 된다 |
| `name_invalid` | 이름이 `[a-z0-9_]{1,40}` 밖 |
| `name_duplicated` | 같은 이름의 규칙이 둘 넘게 — 그 이름의 규칙은 모두 버린다 |
| `service_invalid` | 서비스가 수집이 받는 꼴 밖(비었거나 대문자 · 빈칸 등) |
| `severity_invalid` | 세기가 `info` · `warning` · `critical` 밖 |
| `text_empty` · `text_too_long` · `text_hidden_characters` | 글자가 비었다 · 120자를 넘는다 · 숨은 문자가 들었다 |
| `too_many_patterns` | 맞는 규칙이 이미 20개 |

- **맞춤**: 서비스가 같고 로그의 내용에 글자가 **그대로 들었을 때**(`strpos` — 대소문자 구분 · 정규식도 `LIKE` 도 아니다: `%` · `_` · `.` · `*` 도 글자 그대로).
- **창**: 바퀴마다 규칙마다 따로, 시각(Redis 가 받은 시각)이 `(max(앞에서 본 곳, 지금 − 5분), 지금 − 15초]` 인 줄을 본다(15초는 저장이 따라올 여유 · 받은 때보다 5분 넘게 오래된 줄 — 다시 뜬 뒤의 밀린 묶음 — 은 보지 않는다). 처음 맞으면 그 바퀴에 한 건, 그 뒤 맞은 줄은 쌓아 두었다가 마지막 알림에서 10분이 지난 첫 바퀴에 쌓인 수로 한 건(맞춤이 그친 뒤에도). 규칙마다 질의 둘(수 · 첫 줄) — 서비스 색인으로 기간을 좁힌다. 자기 설정에 없는 규칙의 상태는 지우지 않는다(두 자리의 설정이 배포 중에 달라도). 판정이 5분 넘게 멈췄거나 규칙을 뺐다 다시 넣으면 그 전에 쌓인 수는 버린다 — 오래된 맞춤으로는 알리지 않는다(마지막 알림의 시각은 남아 10분 창은 이어진다).
- **사건**: `kind=log_pattern` · `key=log_pattern:<이름>` · 세기 = 규칙의 세기 · title `로그 글자 규칙 <이름> — <서비스>` · detail `<n>줄 맞음 · 첫 줄 요청 ID <ID|없음> · 시각 <ISO|모름>`. **로그의 내용도 규칙의 글자도 싣지 않는다**(규칙의 글자는 로그 내용의 일부와 같은 글자다). 보내기 · 다시 보내기 · 헬스의 못 보낸 수는 자원 알림과 같다.
- **헬스의 `log_alerts` 칸**에 더한다: `pattern_rules`(쓰는 규칙 수) · `pattern_rules_dropped`(버린 규칙 수) · `pattern_last_judged_at`(이 자리가 마지막으로 잠금을 얻어 판정한 시각) · `pattern_last_failure`(마지막 바퀴의 예외 종류 — 문구는 싣지 않는다).
- **받아들인 한계**: 수는 어림이다 — 저장이 15초보다 늦게 따라온 줄 · 판정이 5분 넘게 멈춘 사이의 줄은 세지 않는다(알림은 "일어났다"를 알리는 것). 판정의 조회는 2초에서 끊긴다(그 바퀴는 `pattern_last_failure` 에 남고 다음 바퀴에 다시 돈다). 같은 이름의 규칙의 글자 · 서비스를 바꿔 5분 안에 다시 띄우면 옛 글자로 센 수가 다음 사건에 한 번 섞인다.

### 알려진 한계

- **알림이 한 번 더 갈 수 있다**: 알림 서버는 같은 key 를 10분 동안만 한 건으로 본다(중복 방지용 고유 번호를 받지 않는다). 관측 서버가 알림을 "보냈지만 보냈다고 적기 전"에 죽고 10분 넘게 지나 돌아오면 그 알림이 한 번 더 간다. 없애려면 알림 서버를 고쳐야 한다.
- **빠른 재발생**: 같은 규칙이 10분 안에 해소됐다 다시 넘으면 알림 서버가 같은 key 를 억제해 두 번째 "넘음"은 발송되지 않는다. `firing` 에는 보인다.
- 거절된 알림(`rejected_alerts`)은 표에 남는다(드물다 — 지우는 예약은 아직 없다).
- **스트림 유실**: 스트림 상한(agent 의 `MAXLEN ~ 20000`, 약 2.3일분)을 넘게 관측 서버가 꺼져 있거나 Redis 가 재시작하면 그 구간의 지표는 사라진다 — 그래프에 빈 구간으로 보인다. 밀린 건수는 헬스의 `metrics.lag` 에.
- 측정 시각이 늦은 항목(뒤처진 시계 · 밀린 항목)은 6일까지 받는다. 그보다 늦은 것은 `too_old` 로 버린다.
- 5분 평균에서 읽는 점은 첫 점이 `from` 보다 최대 5분 앞설 수 있다(구간을 통째로 한 점으로 내므로).
- 큰 간격으로 묶을 때 5분 평균은 구간의 **시작 시각**이 든 칸에 통째로 들어간다(간격이 5분의 배수가 아니면 칸 경계에 걸친 구간이 한쪽에 몰린다).
- `dropped` 는 이 프로세스가 뜬 뒤의 값이다.
- **DB 연결**: 자리 하나가 PostgreSQL 연결을 최대 21개 연다(로그 7 · 로그인 시도 3 · 지표 6 · 판정 · 알림 5). 무중단 배포로 두 자리가 겹치면 42 — 서버 PostgreSQL 의 연결 한도(기본 100)를 배포 때 확인한다.
- 두 자리가 빈 DB 에서 동시에 처음 뜨면 표 만들기가 겹쳐 한쪽의 첫 수집 바퀴가 실패할 수 있다(첫 배포 한 번뿐 — 다음 바퀴에 회복한다).
- 오래 끊긴 서버는 자동으로 빠지지 않는다 — 아래 "서버를 없애거나 이름을 바꿀 때".

### 서버를 없애거나 이름을 바꿀 때

서버의 마지막 수신 기록과 판정 상태는 원본이 지워져도 남으므로, 그 서버는 `GET /metrics/latest` 에 "수신 없음"으로 남고 "지표 끊김"을 1시간마다 다시 알린다. 서버를 없애거나 agent 의 `SERVER_NAME` 을 바꿨으면 `METRICS_EXPECTED_SERVERS` 에서 옛 이름을 빼고 관측 서버를 다시 띄운 뒤, 옛 이름의 기록을 지운다(`docker compose exec postgres psql -U <POSTGRES_USER> <POSTGRES_DB>`):

```sql
BEGIN;
DELETE FROM metric_server_receipt WHERE server = '<옛 서버 이름>';
DELETE FROM resource_rule_state  WHERE server = '<옛 서버 이름>';
DELETE FROM resource_alert_target WHERE server = '<옛 서버 이름>';
COMMIT;
```

아직 보내지 못한 그 서버의 알림까지 버리려면 `DELETE FROM resource_alert_outbox WHERE server = '<옛 서버 이름>';` 도 함께 한다(남겨 두면 알림 서버가 받을 때까지 보낸다). 원본과 5분 평균은 보관 기간이 지나면 저절로 지워진다.

## 무중단 배포와 두 자리

배포 중에는 두 자리(blue·green)가 잠시 함께 떠 있다. 둘 다 수집하지만 결과는 하나일 때와 같다 — **자리별 활성 판정을 두지 않는다**.

- 두 자리는 같은 소비자 그룹을 같은 소비자 이름으로 읽는다. 새 항목은 Redis 가 한쪽에만 준다.
- 한쪽이 저장하다 죽어 확인하지 못한 항목은 같은 이름의 미확인분이라 다른 쪽이 기동 직후와 1분마다 다시 받아 저장한다(따로 넘겨받는 절차가 없다). 그 사이 양쪽이 같은 항목을 저장해도 기본 키가 한 번만 남긴다.
- 30일 삭제(와 로그인 시도 기록 삭제 · 지표 삭제 · 5분 평균)가 두 자리에서 함께 돌아도 결과가 같다.
- 지표도 같은 규칙이다 — 두 자리가 같은 지표 소비자 그룹을 같은 이름으로 읽고, 같은 항목은 한 번만 남는다.
- 로그인 잠금 판정은 두 자리가 같은 DB 의 같은 상태 행을 잡으므로 어느 자리가 받아도 같다.
- 조회는 nginx 가 보내는 자리 하나가 받는다. 두 자리는 같은 DB 를 본다.

## 구동

배포는 ci-cd 파이프라인이 한다(테스트 → 이미지 빌드 → 무중단 전환). agent 는 자리만 띄운다(`docker compose up -d --no-build --no-deps <자리>`) — **postgres 는 띄우지도 건드리지도 않으므로, 처음에 손으로 올려 둔다**. 수동 조작은 ci-cd 저장소 README 의 "무중단 배포" 절을 따른다.

처음 올릴 때(서버에서 한 번):
```
cp .env.example .env    # 값 채우기
mkdir -p deploy/active data/postgres
cp deploy/nginx/nginx.conf deploy/active/nginx.conf
cp deploy/nginx/upstream-blue.conf deploy/active/upstream.conf
docker compose up -d postgres                       # DB 먼저 — healthy 가 될 때까지
docker compose up -d --build observe-blue nginx     # 자리는 postgres 가 healthy 가 된 뒤 뜬다
curl -sD- -o /dev/null http://<LAN 주소>:17000/health | grep -i x-deploy-slot   # blue
```

- PostgreSQL 은 호스트 포트를 열지 않는다. DB 를 직접 볼 때는 `docker compose exec postgres psql -U <POSTGRES_USER> <POSTGRES_DB>`.
- 이름 없이 `docker compose up -d` 를 하면 postgres 와 nginx 만 뜨고 두 자리는 뜨지 않는다(자리에는 profiles 가 붙어 있다).
- DB 파일은 `data/postgres`(git·빌드 컨텍스트 밖)에 있다. 지우면 로그가 전부 사라진다.

## 테스트

- `./gradlew test` — 단위 테스트. 파이프라인의 test 단계(`docker build --target test`)가 이것을 돈다.
- `scripts/engine-test.sh` — 실제 PostgreSQL·Redis 컨테이너에 대고 도는 테스트(`engineTest`). 빌드 안에서는 DB 를 띄울 수 없어 파이프라인에서는 돌지 않는다 — 로컬에서 돌린다.

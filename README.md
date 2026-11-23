# study-note-deploy-system-observe

관측 서버 — 네 서비스(front · backend · llm · 배포 시스템)가 Redis 스트림 `logs` 로 보내는 요청 단위 로그를 받아 30일 동안 보관하고, 조회 API 로 찾아보게 한다. 어드민의 로그인 시도를 기록하고 잠금을 판정한다(아래 "로그인 시도와 잠금"). Kotlin · Spring Boot · PostgreSQL.

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

## 무중단 배포와 두 자리

배포 중에는 두 자리(blue·green)가 잠시 함께 떠 있다. 둘 다 수집하지만 결과는 하나일 때와 같다 — **자리별 활성 판정을 두지 않는다**.

- 두 자리는 같은 소비자 그룹을 같은 소비자 이름으로 읽는다. 새 항목은 Redis 가 한쪽에만 준다.
- 한쪽이 저장하다 죽어 확인하지 못한 항목은 같은 이름의 미확인분이라 다른 쪽이 기동 직후와 1분마다 다시 받아 저장한다(따로 넘겨받는 절차가 없다). 그 사이 양쪽이 같은 항목을 저장해도 기본 키가 한 번만 남긴다.
- 30일 삭제(와 로그인 시도 기록 삭제)가 두 자리에서 함께 돌아도 결과가 같다.
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

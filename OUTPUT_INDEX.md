# 출력 구조

## 코드

- 메인 자동화 코드: `tools/`
- 테스트: `tests/`
- Codex 스킬: `codex-skills/`
- 운영 규칙: `AGENTS.md`
- 작업 계획 메모: `plan.md`

## 생성물

- 에피소드 작업공간: `episode-01/`, `episode-02/`, ...
- 세그먼트 비디오: 각 작업공간의 `segments/`, `clips/`, `videos/` 또는 manifest가 가리키는 경로
- Gemini 생성 SRT: 각 작업공간의 `raw_speech_only/pass1~passN/`
- 최종 병합 SRT: 각 작업공간의 `merged_speech_only/`

## 운영 상태

- `.codex/` 안의 `state.json`, `rollout_result.json`, `PROGRESS.md`
- same-session recovery 상태 파일

## 정리 대상

- `__pycache__/`
- 임시 로그 파일
- `.codex/screenshots/`
- 디버그용 임시 산출물

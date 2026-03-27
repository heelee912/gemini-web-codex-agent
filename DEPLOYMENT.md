# Deployment

## 목표

이 저장소는 이제 특정 Windows 계정명, 특정 로컬 드라이브 문자, 특정 비디오 파일명에 묶이지 않는 형태를 기본값으로 둡니다.

## 권장 설치 순서

1. Python 3.11 이상 설치
2. `setup.cmd`
3. 필요하면 `rollout.cmd setup --workspace-root <DATA_ROOT> --reference-dir <REFERENCE_DIR>`
4. `rollout.cmd check`
5. `rollout.cmd run-supervisor --thread-id <THREAD_ID>`

## 핵심 환경변수

- `ROLLOUT_WORKSPACE_ROOT`
  - 에피소드 작업공간이 있는 루트입니다.
  - 비워 두면 레포 루트를 기준으로 찾습니다.
- `ROLLOUT_REFERENCE_DIR`
  - 참조 자막 폴더입니다.
- `ROLLOUT_PROMPT_PATH`
  - 기본 프롬프트 파일입니다.
- `ROLLOUT_STRICT_PROMPT_PATH`
  - strict retry용 프롬프트 파일입니다.
- `GEMINI_CHROME_PATH`
  - Chrome 자동 탐색이 실패할 때만 지정하시면 됩니다.
- `CODEX_COMMAND`
  - `codex`가 PATH에 없을 때만 지정하시면 됩니다.

## 세그먼트 비디오 탐색 규칙

세그먼트 비디오 파일 경로는 코드에 고정하지 않습니다. 아래 순서로 찾습니다.

1. manifest의 `path`, `file`, `filename`, `source`, `src`, `video`, `videoPath`, `media` 계열 필드
2. 사용자가 지정한 세그먼트 디렉터리
3. 작업공간의 `segments`, `clips`, `videos` 폴더
4. 작업공간 루트

파일명도 `s01e01_seg01.mp4`만 강제하지 않고, `seg01`, `segment01`, `clip01`, `part01` 형태를 자동 탐색합니다.

## 작업공간 탐색 규칙

에피소드 작업공간은 다음 중 하나만 있어도 후보로 잡습니다.

- `manifest.json` 또는 `*.manifest.json`
- `segments/`
- `raw_speech_only/`

즉, 데이터가 레포 밖의 `<DATA_ROOT>/episode-01` 같은 경로에 있어도 동작합니다.

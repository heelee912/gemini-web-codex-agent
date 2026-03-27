# Subtitle Rollout Tool

Windows 데스크톱에서 Gemini UI 자동화를 통해 자막 생성 작업을 돌리는 도구입니다.

이 저장소는 이제 특정 사용자 계정명, 특정 드라이브, 특정 비디오 폴더 위치를 전제하지 않습니다. 작업 데이터는 레포 안이나 레포 밖 어느 경로에 두셔도 됩니다.

## 빠른 시작

1. Python 3.11 이상을 설치합니다.
2. 일반 사용자는 아래 한 줄로 초기 설정과 의존성 설치를 시작하시면 됩니다.

```powershell
setup.cmd
```

3. 작업 데이터를 레포 밖에 두고 싶다면:

```powershell
rollout.cmd setup --workspace-root <DATA_ROOT> --reference-dir <REFERENCE_DIR>
```

4. 현재 PC에서 실행 가능한지 점검합니다.

```powershell
rollout.cmd check
```

5. Codex thread id를 넣어 supervisor를 시작합니다.

```powershell
rollout.cmd run-supervisor --thread-id <THREAD_ID>
```

## 일반화된 경로 규칙

- 작업 데이터 루트는 `ROLLOUT_WORKSPACE_ROOT`로 바꿀 수 있습니다.
- 참조 자막 폴더는 `ROLLOUT_REFERENCE_DIR`로 바꿀 수 있습니다.
- 프롬프트 파일은 `ROLLOUT_PROMPT_PATH`, `ROLLOUT_STRICT_PROMPT_PATH`로 바꿀 수 있습니다.
- 세그먼트 비디오 위치는 manifest 안의 `path/file/video/src` 같은 필드를 우선 읽고, 없으면 작업공간 안에서 자동 탐색합니다.
- 상태 파일은 데이터가 레포 밖에 있더라도 절대 경로로 안전하게 기록됩니다.

## 주요 파일

- 일반 사용자 진입점: `rollout.cmd`
- 1회 초기 설정 래퍼: `setup.cmd`
- 관리 CLI: `tools/rollout_cli.py`
- 런타임 설정 탐색: `tools/runtime_config.py`
- 환경설정 예시: `rollout.env.example`
- 배포 설명: `DEPLOYMENT.md`

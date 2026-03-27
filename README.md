

# Subtitle Rollout Tool
<img width="1203" height="677" alt="image" src="https://github.com/user-attachments/assets/dc682806-931f-4106-bf3d-588a39f25629" />


Windows 데스크톱에서 Gemini UI 자동화 워커와 codex cli의 루프를 통해 자막 생성 작업을 돌리는 도구입니다.

이 저장소는 이제 특정 사용자 계정명, 특정 드라이브, 특정 비디오 폴더 위치를 전제하지 않습니다. 작업 데이터는 레포 안이나 레포 밖 어느 경로에 두셔도 됩니다.

완전한 배포본이 아니라 에이전트에게 환경에 맞게 수정해달라고 할 필요가 있습니다.

segment를 나눌 필요성, 사전 whisper 자막 필요성에 대해서 에이전트와 상담하세요.


필요성

단순히 서브에이전트에게 맡기면 턴이 영원히 돌 수가 없으니까 한 턴에 걸리는 시간 내에 자막 몇개밖에 생성해오지 못한다는 것,

단순히 파이썬으로 자동화하면 도중에 생기는 다양한 문제에 대응할 수 없다는 것,

크론으로 돌리는 것보다 좀 더 구조화하고 싶어서 실험적으로 만들어진 것입니다.

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

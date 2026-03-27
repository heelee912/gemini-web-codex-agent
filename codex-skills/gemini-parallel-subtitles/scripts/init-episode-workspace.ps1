param(
    [Parameter(Mandatory = $true)]
    [string]$EpisodeDir,
    [Parameter(Mandatory = $true)]
    [string]$SegmentsDir,
    [Parameter(Mandatory = $true)]
    [string]$Prefix
)

$ErrorActionPreference = 'Stop'

New-Item -ItemType Directory -Force (Join-Path $EpisodeDir 'raw_speech_only') | Out-Null
New-Item -ItemType Directory -Force (Join-Path $EpisodeDir 'episode_passes') | Out-Null
New-Item -ItemType Directory -Force (Join-Path $EpisodeDir 'merged_speech_only') | Out-Null

$segments = @()
Get-ChildItem $SegmentsDir -Filter "$Prefix`_seg*.mp4" | Sort-Object Name | ForEach-Object {
    $duration = & ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 $_.FullName
    $durationMs = [int][Math]::Round([double]::Parse($duration, [System.Globalization.CultureInfo]::InvariantCulture) * 1000)
    $subtitleFile = ($_.BaseName + '.srt')
    $segments += [pscustomobject]@{
        file = $_.Name
        subtitleFile = $subtitleFile
        durationMs = $durationMs
    }
}

$manifest = [pscustomobject]@{
    segments = $segments
}

$manifestPath = Join-Path $EpisodeDir ($Prefix + '.manifest.json')
$manifest | ConvertTo-Json -Depth 4 | Set-Content -Encoding UTF8 $manifestPath
Write-Output $manifestPath

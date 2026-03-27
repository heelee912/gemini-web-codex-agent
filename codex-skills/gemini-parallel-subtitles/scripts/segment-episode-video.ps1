param(
    [Parameter(Mandatory = $true)]
    [string]$InputVideo,
    [Parameter(Mandatory = $true)]
    [string]$OutputDir,
    [Parameter(Mandatory = $true)]
    [string]$Prefix,
    [int]$SegmentSeconds = 210
)

$ErrorActionPreference = 'Stop'

New-Item -ItemType Directory -Force $OutputDir | Out-Null

$duration = & ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 $InputVideo
$durationSeconds = [double]::Parse($duration, [System.Globalization.CultureInfo]::InvariantCulture)
$segmentCount = [int][Math]::Ceiling($durationSeconds / $SegmentSeconds)

for ($i = 0; $i -lt $segmentCount; $i++) {
    $start = $i * $SegmentSeconds
    $remaining = [Math]::Max(0, $durationSeconds - $start)
    if ($remaining -le 0) { break }

    $length = [Math]::Min($SegmentSeconds, $remaining)
    $name = '{0}_seg{1:00}.mp4' -f $Prefix, ($i + 1)
    $outPath = Join-Path $OutputDir $name

    if (Test-Path $outPath) {
        Write-Output ('SKIP ' + $outPath)
        continue
    }

    $cmd = 'ffmpeg -y -ss "{0}" -i "{1}" -t "{2}" -threads 4 -c:v libx264 -preset ultrafast -crf 22 -c:a aac -b:a 160k "{3}" >nul 2>nul' -f $start, $InputVideo, $length, $outPath
    cmd.exe /c $cmd | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw ("ffmpeg failed for " + $outPath)
    }

    Write-Output $outPath
}

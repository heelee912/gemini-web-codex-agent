param(
    [Parameter(Mandatory = $true)]
    [string]$ManifestPath,
    [Parameter(Mandatory = $true)]
    [string]$RawDir,
    [Parameter(Mandatory = $true)]
    [string]$OutputPath
)

$ErrorActionPreference = 'Stop'

function Read-TextUtf8 {
    param([string]$Path)
    [System.IO.File]::ReadAllText($Path, [System.Text.Encoding]::UTF8)
}

function Parse-Srt {
    param([string]$Path)

    $text = Read-TextUtf8 -Path $Path
    $blocks = [regex]::Split($text.Trim(), "\r?\n\r?\n")
    $cues = @()

    foreach ($block in $blocks) {
        if ([string]::IsNullOrWhiteSpace($block)) { continue }
        $lines = $block -split "\r?\n"
        if ($lines.Length -lt 3) { continue }
        $match = [regex]::Match($lines[1], '(\d\d):(\d\d):(\d\d),(\d\d\d) --> (\d\d):(\d\d):(\d\d),(\d\d\d)')
        if (-not $match.Success) { continue }

        $startMs = ([int]$match.Groups[1].Value * 3600 + [int]$match.Groups[2].Value * 60 + [int]$match.Groups[3].Value) * 1000 + [int]$match.Groups[4].Value
        $endMs = ([int]$match.Groups[5].Value * 3600 + [int]$match.Groups[6].Value * 60 + [int]$match.Groups[7].Value) * 1000 + [int]$match.Groups[8].Value
        $textLines = $lines[2..($lines.Length - 1)]

        $cues += [pscustomobject]@{
            StartMs = $startMs
            EndMs   = $endMs
            Text    = ($textLines -join "`r`n")
        }
    }

    return ,$cues
}

function Format-SrtTime {
    param([int]$Ms)
    if ($Ms -lt 0) { $Ms = 0 }
    $ts = [TimeSpan]::FromMilliseconds($Ms)
    '{0:00}:{1:00}:{2:00},{3:000}' -f [int]$ts.TotalHours, $ts.Minutes, $ts.Seconds, $ts.Milliseconds
}

function Write-Srt {
    param(
        [object[]]$Cues,
        [string]$Path
    )

    $builder = New-Object System.Text.StringBuilder
    $index = 1
    foreach ($cue in $Cues) {
        [void]$builder.AppendLine($index)
        [void]$builder.AppendLine(('{0} --> {1}' -f (Format-SrtTime $cue.StartMs), (Format-SrtTime $cue.EndMs)))
        [void]$builder.AppendLine($cue.Text)
        [void]$builder.AppendLine()
        $index++
    }

    $dir = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($dir)) {
        New-Item -ItemType Directory -Force $dir | Out-Null
    }
    [System.IO.File]::WriteAllText($Path, $builder.ToString(), [System.Text.Encoding]::UTF8)
}

$manifest = Read-TextUtf8 -Path $ManifestPath | ConvertFrom-Json
$merged = @()
$offsetMs = 0

for ($i = 0; $i -lt $manifest.segments.Count; $i++) {
    $segment = $manifest.segments[$i]
    $fileName = if ($segment.subtitleFile) { [string]$segment.subtitleFile } else { [string]$segment.file }
    $segmentPath = Join-Path $RawDir $fileName
    $cues = Parse-Srt -Path $segmentPath

    foreach ($cue in $cues) {
        $merged += [pscustomobject]@{
            StartMs = $cue.StartMs + [int]$offsetMs
            EndMs   = $cue.EndMs + [int]$offsetMs
            Text    = $cue.Text
        }
    }

    $offsetMs += [int]$segment.durationMs
}

Write-Srt -Cues $merged -Path $OutputPath
Write-Output $OutputPath

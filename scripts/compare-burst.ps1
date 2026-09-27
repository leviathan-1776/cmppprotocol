param(
    [Parameter(Mandatory=$true)][string]$CandidatePath,
    [string]$BaselinePath = "target/burst-optimization/baseline.exe",
    [Parameter(Mandatory=$true)][string]$OutputDirectory,
    [ValidateSet("Screen", "Normal", "Stability", "Allocation")][string]$Set = "Screen",
    [int]$Duration = 5,
    [int]$Pairs = 3,
    [int]$StartPair = 1,
    [string]$Only = ""
)
$ErrorActionPreference = "Stop"
if ($Pairs -lt 1 -or $StartPair -lt 1) { throw "Pairs and StartPair must be positive" }
$scenarios = switch ($Set) {
    "Screen" { @("nagle-deliver", "nagle-mixed", "short-c4-w256", "short-c1-w256", "deliver-default") }
    "Normal" { @("short-c1-w16", "short-c1-w64", "short-c1-w256", "short-c4-w64", "rtt50-c1-w256", "long-synthetic", "deliver-default", "mixed-default") }
    "Stability" { @("nagle-deliver", "nagle-mixed", "short-c4-w256") }
    "Allocation" { @("alloc-short", "alloc-long", "alloc-deliver") }
}
$root = Split-Path $PSScriptRoot -Parent
$output = Join-Path $root $OutputDirectory
New-Item -ItemType Directory -Force -Path $output | Out-Null
if (Test-Path -LiteralPath (Join-Path $output "paired-results.csv")) { throw "Use a new output directory to preserve prior results" }
$rows = @()
foreach ($scenario in $scenarios) {
    if ($Only -and $scenario -notmatch $Only) { continue }
    for ($pair = $StartPair; $pair -lt ($StartPair + $Pairs); $pair++) {
        $order = if ($Set -eq "Stability") { @("candidate") } elseif ($pair % 2 -eq 1) { @("baseline", "candidate") } else { @("candidate", "baseline") }
        foreach ($variant in $order) {
            $binary = if ($variant -eq "baseline") { $BaselinePath } else { $CandidatePath }
            $directory = "$OutputDirectory/$scenario-$pair-$variant"
            & "$PSScriptRoot/measure-loadtest.ps1" -Duration $Duration -Repeats 1 -Only "^$scenario`$" -BinaryPath $binary -OutputDirectory $directory
            $csv = Join-Path $root "$directory/results.csv"
            foreach ($row in (Import-Csv -LiteralPath $csv)) {
                $row | Add-Member -NotePropertyName pair -NotePropertyValue $pair
                $row | Add-Member -NotePropertyName variant -NotePropertyValue $variant
                $row | Add-Member -NotePropertyName binary_sha256 -NotePropertyValue (Get-FileHash -LiteralPath (Join-Path $root $binary) -Algorithm SHA256).Hash
                $rows += $row
            }
            $rows | Export-Csv -LiteralPath (Join-Path $output "paired-results.csv") -NoTypeInformation -Encoding UTF8
            Write-Output "PAIR scenario=$scenario pair=$pair variant=$variant"
        }
    }
}

if ($rows.Count -eq 0) { throw "No scenarios matched" }

param(
    [int]$Duration = 3,
    [int]$Repeats = 2,
    [string]$OutputDirectory = "target/performance-matrix",
    [string]$Only = ""
)
$ErrorActionPreference = "Stop"
if ($Duration -lt 1 -or $Repeats -lt 1) { throw "Duration and Repeats must be positive" }
$root = Split-Path $PSScriptRoot -Parent
$binary = Join-Path $root "target/release/examples/loadtest.exe"
if (!(Test-Path -LiteralPath $binary)) { throw "Run cargo build --release --example loadtest first" }
$output = [IO.Path]::GetFullPath((Join-Path $root $OutputDirectory))
New-Item -ItemType Directory -Force -Path $output | Out-Null
# 顺序运行，避免场景相互争抢 CPU；每个进程内部实现真正的多连接并发。
$scenarios = @(
    @{ Name="short-c1-w16"; Args="connections=1 window=16 delay_ms=0" },
    @{ Name="short-c1-w64"; Args="connections=1 window=64 delay_ms=0" },
    @{ Name="short-c1-w256"; Args="connections=1 window=256 delay_ms=0" },
    @{ Name="short-c4-w64"; Args="connections=4 window=64 delay_ms=0" },
    @{ Name="short-c4-w256"; Args="connections=4 window=256 delay_ms=0" },
    @{ Name="rtt50-c1-w64"; Args="connections=1 window=64 delay_ms=50" },
    @{ Name="rtt50-c1-w256"; Args="connections=1 window=256 delay_ms=50" },
    @{ Name="rtt50-c4-w64"; Args="connections=4 window=64 delay_ms=50" },
    @{ Name="long-default"; Args="window=256 delay_ms=0 long=1" },
    @{ Name="long-synthetic"; Args="window=256 delay_ms=0 long=1 udh_cooldown_ms=1" },
    @{ Name="deliver-default"; Args="window=64 delay_ms=0 parallel=0 deliver_window=32" },
    @{ Name="mixed-default"; Args="window=64 delay_ms=0 deliver_window=32" },
    @{ Name="deliver-spool1024"; Args="window=64 delay_ms=0 parallel=0 deliver_window=32 spool_capacity=1024" },
    @{ Name="mixed-spool1024"; Args="window=64 delay_ms=0 deliver_window=32 spool_capacity=1024" },
    @{ Name="nagle-deliver"; Args="window=64 delay_ms=0 parallel=0 deliver_window=32 server_nodelay=0" },
    @{ Name="nagle-mixed"; Args="window=64 delay_ms=0 deliver_window=32 server_nodelay=0" },
    @{ Name="short-c4-w256-spool1024"; Args="connections=4 window=256 delay_ms=0 spool_capacity=1024" },
    @{ Name="nagle-control-w16"; Args="window=16 delay_ms=0 server_nodelay=0" },
    @{ Name="alloc-short"; Args="window=256 delay_ms=0 alloc=1 spool_capacity=1024" },
    @{ Name="alloc-long"; Args="window=256 delay_ms=0 long=1 udh_cooldown_ms=1 alloc=1 spool_capacity=1024" },
    @{ Name="alloc-deliver"; Args="window=64 delay_ms=0 parallel=0 deliver_window=32 alloc=1 spool_capacity=1024" }
)
$metadata = @{
    Timestamp = (Get-Date -Format o)
    LogicalProcessors = [Environment]::ProcessorCount
    BinarySha256 = (Get-FileHash -LiteralPath $binary -Algorithm SHA256).Hash
    Duration = $Duration
    Repeats = $Repeats
    Scope = "Whole process: client + mock gateway + harness; allocation rounds separate"
}
$metadata | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $output "environment.json") -Encoding UTF8
if (!("LoadtestCpuTime" -as [type])) {
    Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;
public static class LoadtestCpuTime {
    [DllImport("kernel32.dll", SetLastError=true)]
    public static extern bool GetProcessTimes(IntPtr process, out long creation, out long exit, out long kernel, out long user);
}
"@
}
$rows = @()
foreach ($scenario in $scenarios) {
    if ($Only -and $scenario.Name -notmatch $Only) { continue }
    for ($round = 1; $round -le $Repeats; $round++) {
        $prefix = Join-Path $output "$($scenario.Name)-$round"
        $arguments = "duration=$Duration $($scenario.Args)"
        $timer = [Diagnostics.Stopwatch]::StartNew()
        $process = Start-Process -FilePath $binary -ArgumentList $arguments -WindowStyle Hidden -PassThru -RedirectStandardOutput "$prefix.log" -RedirectStandardError "$prefix.err"
        # 保持句柄以便进程退出后读取 CPU 时间；峰值工作集只采样存活进程。
        $handle = $process.Handle
        $peakWorkingSet = 0L
        while (!$process.HasExited) {
            $process.Refresh()
            $peakWorkingSet = [Math]::Max($peakWorkingSet, $process.WorkingSet64)
            if ($timer.Elapsed.TotalSeconds -gt ($Duration + 90)) {
                $process.Kill()
                throw "Measurement process exceeded timeout: $prefix"
            }
            Start-Sleep -Milliseconds 50
        }
        $process.WaitForExit()
        $timer.Stop()
        $created = 0L; $exited = 0L; $kernel = 0L; $user = 0L
        if (![LoadtestCpuTime]::GetProcessTimes($handle, [ref]$created, [ref]$exited, [ref]$kernel, [ref]$user)) {
            throw "GetProcessTimes failed: $([Runtime.InteropServices.Marshal]::GetLastWin32Error())"
        }
        $cpuSeconds = ($kernel + $user) / 10000000.0
        $row = [ordered]@{ scenario=$scenario.Name; round=$round; arguments=$arguments; exit_code=$process.ExitCode; wall_seconds=$timer.Elapsed.TotalSeconds; cpu_seconds=$cpuSeconds; average_cpu_cores=$cpuSeconds/$timer.Elapsed.TotalSeconds; sampled_peak_working_set_bytes=$peakWorkingSet }
        foreach ($key in "connections segments responses submit_rate deliver_sent deliver_acked delivers deliver_rate retries loss_signals timeouts closed exhausted latency_samples p50_us p95_us p99_us observed_secs alloc_enabled allocations allocated_bytes deliver_samples deliver_p50_us deliver_p95_us deliver_p99_us".Split(" ")) { $row[$key] = $null }
        $row["event_depth_peak_max"] = 0L
        foreach ($line in (Get-Content -LiteralPath "$prefix.log")) {
            if ($line -match "event_depth_peak=(\d+)") {
                $row["event_depth_peak_max"] = [Math]::Max($row["event_depth_peak_max"], [long]$Matches[1])
            }
            if ($line.StartsWith("RESULT ")) {
                foreach ($field in $line.Substring(7).Split(" ")) {
                    $kv = $field.Split("=", 2)
                    $row[$kv[0]] = $kv[1]
                }
            }
            if ($line.StartsWith("DELIVER_LATENCY ")) {
                foreach ($field in $line.Substring(16).Split(" ")) {
                    $kv = $field.Split("=", 2)
                    $row["deliver_" + $kv[0]] = $kv[1]
                }
            }
        }
        $rows += [pscustomobject]$row
        $rows | Export-Csv -LiteralPath (Join-Path $output "results.csv") -NoTypeInformation -Encoding UTF8
        Write-Output "$($scenario.Name) round=$round exit=$($process.ExitCode) cpu_seconds=$cpuSeconds submit_rate=$($row.submit_rate) deliver_rate=$($row.deliver_rate)"
        $process.Dispose()
    }
}

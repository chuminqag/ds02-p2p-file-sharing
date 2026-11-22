$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $projectRoot
$pythonPath = Join-Path $projectRoot '.runtime/python/python.exe'
if ($env:DS02_PYTHON) { $pythonPath = $env:DS02_PYTHON }
$runRoot = Join-Path $projectRoot ('tmp/multiprocess-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Force $runRoot | Out-Null
$env:TEMP = $runRoot
$env:TMP = $runRoot
$env:PYTHONNOUSERSITE = '1'
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONIOENCODING = 'utf-8'
$keysJson = & $pythonPath -B -s -m scripts.generate_keys
$keys = $keysJson | ConvertFrom-Json
$env:DS02_SWARM_KEY = [Guid]::NewGuid().ToString('N')
$env:DS02_TRACKER_HOST = '127.0.0.1'
$env:DS02_PEER_HOST = '127.0.0.1'
$env:DS02_LEASE_SECONDS = '5'
$env:DS02_CHUNK_SIZE = '65536'
$env:DS02_IO_TIMEOUT = '1'
$env:DS02_DOWNLOAD_WORKERS = '2'
$env:DS02_CHUNK_RETRIES = '20'
$children = [System.Collections.Generic.List[object]]::new()

function Free-Port {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $port = $listener.LocalEndpoint.Port
    $listener.Stop()
    return $port
}

function Launch-Node($module, $name) {
    $log = Join-Path $runRoot ($name + '.log')
    $process = Start-Process -FilePath $pythonPath -ArgumentList @('-B', '-s', '-m', $module) `
        -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $runRoot ($name + '.out')) -RedirectStandardError $log
    $node = [pscustomobject]@{ Process = $process; Log = $log; Name = $name }
    $children.Add($node)
    return $node
}

function Launch-Tracker($name) {
    $env:DS02_PEER_KEYS = $keysJson
    try {
        return Launch-Node 'src.tracker' $name
    } finally {
        Remove-Item Env:DS02_PEER_KEYS -ErrorAction SilentlyContinue
    }
}

function Wait-Log($node, $pattern, $seconds = 40) {
    $deadline = [DateTime]::UtcNow.AddSeconds($seconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        $content = if (Test-Path $node.Log) { Get-Content -LiteralPath $node.Log -Raw -Encoding UTF8 } else { '' }
        if ($content -match $pattern) { return }
        if ($node.Process.HasExited) { throw "$($node.Name) exited before $pattern; see $($node.Log)" }
        Start-Sleep -Milliseconds 50
    }
    throw "Timeout waiting for $pattern in $($node.Log)"
}

function Stop-Node($node) {
    if (-not $node.Process.HasExited) {
        Stop-Process -Id $node.Process.Id -Force
        $node.Process.WaitForExit(5000) | Out-Null
    }
}

function Launch-Peer($number, $action, $name, $delay = '0.05', $corrupt = '-1', $workers = '2') {
    $env:DS02_PEER_ID = "peer$number"
    $env:DS02_PEER_TOKEN = $keys."peer$number"
    $env:DS02_PEER_PORT = [string](Free-Port)
    $env:DS02_STORAGE = Join-Path $runRoot $name
    $env:DS02_ACTION = $action
    $env:DS02_UPLOAD_DELAY = $delay
    $env:DS02_CORRUPT_INDEX = $corrupt
    $env:DS02_DOWNLOAD_WORKERS = $workers
    return Launch-Node 'src.peer' $name
}

try {
    $source = Join-Path $runRoot 'demo.bin'
    $bytes = New-Object byte[] (4 * 1024 * 1024 + 123)
    $random = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    $random.GetBytes($bytes)
    $random.Dispose()
    [System.IO.File]::WriteAllBytes($source, $bytes)
    $fileId = (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant()
    $env:DS02_FILE = $source
    $env:DS02_FILE_ID = $fileId
    $env:DS02_TRACKER_PORT = [string](Free-Port)
    $tracker = Launch-Tracker 'tracker-failover'
    Wait-Log $tracker '\[LISTEN\]'
    $first = Launch-Peer 1 'seed' 'failover-peer1' '0.15' '0'
    Wait-Log $first '\[SEED\]'
    $second = Launch-Peer 2 'seed' 'failover-peer2' '0.15'
    Wait-Log $second '\[SEED\]'
    $third = Launch-Peer 3 'download' 'failover-peer3'
    Wait-Log $third '\[HASH_REJECT\]'
    Wait-Log $third '\[DOWNLOAD_START\] \[chunk=10 source=peer1\]'
    Stop-Node $first
    $networkFailure = '\[SOURCE_FAILED\] \[chunk=(\d+) source=peer1 (?:TimeoutError|ConnectionResetError|ConnectionRefusedError|ConnectionError)\]'
    Wait-Log $third $networkFailure
    Wait-Log $third '\[FILE_OK\]'
    $destination = Join-Path $runRoot "failover-peer3/$fileId/download.bin"
    if ((Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant() -ne $fileId) {
        throw 'Failover output hash mismatch'
    }
    $log = Get-Content -LiteralPath $third.Log -Raw -Encoding UTF8
    if ($log -notmatch '\[CHUNK_OK\].*source=peer1' -or $log -notmatch '\[CHUNK_OK\].*source=peer2') {
        throw 'Expected two download sources'
    }
    if ($log -notmatch '\[CHUNK_OK\] \[chunk=0 source=peer2\]') {
        throw 'The corrupt chunk was not recovered from peer2'
    }
    $switched = [regex]::Matches($log, $networkFailure) | Where-Object {
        $success = '\[CHUNK_OK\] \[chunk={0} source=peer2\]' -f $_.Groups[1].Value
        $log.Substring($_.Index) -match $success
    } | Select-Object -First 1
    if (-not $switched) {
        throw 'No chunk completed from peer2 after a network failure from peer1'
    }
    Write-Output 'PASS independent processes: two sources, hash rejection, source killed, final SHA-256'
    Stop-Node $second
    Stop-Node $third
    Stop-Node $tracker

    $env:DS02_TRACKER_PORT = [string](Free-Port)
    $tracker = Launch-Tracker 'tracker-overlap'
    Wait-Log $tracker '\[LISTEN\]'
    $first = Launch-Peer 1 'seed' 'overlap-peer1' '0.08'
    Wait-Log $first '\[SEED\]'
    $second = Launch-Peer 2 'download' 'overlap-peer2'
    Wait-Log $second '\[CHUNK_OK\] \[chunk=1 '
    $third = Launch-Peer 3 'download' 'overlap-peer3' '0.02' '-1' '2'
    Wait-Log $second 'to=peer3 complete=False OK'
    Wait-Log $second '\[FILE_OK\]'
    Wait-Log $third '\[FILE_OK\]'
    foreach ($name in @('overlap-peer2', 'overlap-peer3')) {
        $destination = Join-Path $runRoot "$name/$fileId/download.bin"
        if ((Get-FileHash -LiteralPath $destination -Algorithm SHA256).Hash.ToLowerInvariant() -ne $fileId) {
            throw "$name output hash mismatch"
        }
    }
    Write-Output 'PASS independent processes: simultaneous downloads and upload before completion'
    Write-Output "Evidence: $runRoot"
} finally {
    foreach ($node in $children) { Stop-Node $node }
    Remove-Item Env:DS02_PEER_TOKEN -ErrorAction SilentlyContinue
    Remove-Item Env:DS02_PEER_KEYS -ErrorAction SilentlyContinue
    Remove-Item Env:DS02_SWARM_KEY -ErrorAction SilentlyContinue
}

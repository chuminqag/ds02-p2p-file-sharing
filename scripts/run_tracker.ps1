$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONNOUSERSITE = '1'
$env:TEMP = Join-Path (Get-Location) 'tmp'
$env:TMP = $env:TEMP
New-Item -ItemType Directory -Force $env:TEMP | Out-Null
$pythonPath = Join-Path (Get-Location) '.runtime/python/python.exe'
if ($env:DS02_PYTHON) { $pythonPath = $env:DS02_PYTHON }
if (-not (Test-Path -LiteralPath $pythonPath)) { throw 'Set DS02_PYTHON to Python on drive D.' }
if (-not $env:DS02_PEER_KEYS) { throw 'Set DS02_PEER_KEYS before starting tracker. See README.md.' }
& $pythonPath -B -s -m src.tracker
exit $LASTEXITCODE

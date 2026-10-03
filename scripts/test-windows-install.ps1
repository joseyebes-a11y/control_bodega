$ErrorActionPreference = 'Stop'
if ($env:GITHUB_ACTIONS -ne 'true' -or $env:RUNNER_OS -ne 'Windows') {
  throw 'Run this destructive installation test only on an isolated GitHub-hosted Windows runner.'
}
$version = (Get-Content "$PSScriptRoot\..\package.json" -Raw | ConvertFrom-Json).version
$setup = (Resolve-Path "$PSScriptRoot\..\dist-desktop\MicroCellerStudio-$version-Windows-x64-Setup.exe").Path
$installDir = Join-Path $env:RUNNER_TEMP 'MicroCeller Installer Test'
$dataDir = Join-Path $env:APPDATA 'MicroCellerStudio'
$uninstallRoot = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall'
function Get-AppRegistration {
  if (!(Test-Path $uninstallRoot)) { return }
  @(Get-ChildItem $uninstallRoot | Get-ItemProperty | Where-Object { $_.DisplayName -match '^MicroCellerStudio(?: \d+\.\d+\.\d+)?$' })
}
function Run-Setup {
  $process = Start-Process -FilePath $setup -ArgumentList "/S /currentuser /D=$installDir" -Wait -PassThru
  if ($process.ExitCode -ne 0) { throw "Installer failed: $($process.ExitCode)" }
  if (!(Test-Path "$installDir\MicroCellerStudio.exe")) { throw 'Application missing after installation' }
  $entry = Get-AppRegistration
  if ($entry.Count -ne 1 -or $entry[0].DisplayVersion -ne $version) { throw 'Incorrect Windows uninstall registration' }
}
if ((Test-Path $dataDir) -or (Get-AppRegistration).Count -gt 0) { throw 'Runner already has MicroCellerStudio data or an installation' }
New-Item -ItemType Directory -Path "$dataDir\data\uploads", "$dataDir\copias-completas" -Force | Out-Null
# Byte-preservation sentinels: the application is not started with this fake DB.
$sentinels = @('desktop-settings.json', 'data\bodega.db', 'data\uploads\test.txt', 'copias-completas\backup.zip')
$hashes = @{}
foreach ($file in $sentinels) {
  Set-Content -LiteralPath (Join-Path $dataDir $file) -Value "preserve-$file-$([guid]::NewGuid())" -NoNewline
  $hashes[$file] = (Get-FileHash -LiteralPath (Join-Path $dataDir $file)).Hash
}
function Assert-DataPreserved {
  foreach ($file in $sentinels) {
    if ((Get-FileHash -LiteralPath (Join-Path $dataDir $file)).Hash -ne $hashes[$file]) { throw "User data changed: $file" }
  }
}
Run-Setup
Assert-DataPreserved
Write-Output 'PASS: fresh installation, registry, data preservation'

# Simulate the shipped 1.2.2 defect without executing the broken uninstaller.
$entry = (Get-AppRegistration)[0]
Set-ItemProperty -LiteralPath $entry.PSPath -Name DisplayVersion -Value '1.2.2'
$uninstaller = Join-Path $installDir 'Uninstall MicroCellerStudio.exe'
$bytes = [IO.File]::ReadAllBytes($uninstaller)
$bytes[$bytes.Length - 1] = $bytes[$bytes.Length - 1] -bxor 1
[IO.File]::WriteAllBytes($uninstaller, $bytes)
Run-Setup
Assert-DataPreserved
Write-Output 'PASS: update repairs legacy uninstaller with invalid CRC'

Run-Setup
Assert-DataPreserved
Write-Output 'PASS: repeated installation'

$process = Start-Process -FilePath $uninstaller -ArgumentList '/S /currentuser --delete-app-data' -Wait -PassThru
Start-Sleep -Seconds 2
if (!(Test-Path "$installDir\MicroCellerStudio.exe")) { throw 'Unsafe deletion flag was accepted' }
Assert-DataPreserved
Write-Output 'PASS: unsafe user-data removal is rejected'

$process = Start-Process -FilePath $uninstaller -ArgumentList '/S /currentuser' -Wait -PassThru
$deadline = (Get-Date).AddSeconds(30)
while ((Test-Path "$installDir\MicroCellerStudio.exe") -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 200 }
if ((Test-Path $installDir) -or (Get-AppRegistration).Count -ne 0) { throw 'Uninstall left application files or registry entries' }
if (Test-Path "$env:USERPROFILE\Desktop\MicroCellerStudio.lnk") { throw 'Uninstall left desktop shortcut' }
if (Test-Path "$env:APPDATA\Microsoft\Windows\Start Menu\Programs\MicroCellerStudio.lnk") { throw 'Uninstall left Start menu shortcut' }
Assert-DataPreserved
Write-Output 'PASS: uninstall removes program, shortcuts and registration; retains all user data'
Run-Setup
Assert-DataPreserved
Write-Output 'PASS: reinstall after uninstall retains data'

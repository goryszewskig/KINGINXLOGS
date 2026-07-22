param(
    [Parameter(Mandatory = $true)][string]$LocalDir,
    [Parameter(Mandatory = $true)][string]$LogDate
)

$bucket = $env:GCS_BUCKET
$prefix = if ($env:GCS_PREFIX) { $env:GCS_PREFIX } else { "nginx-logs" }
if (-not $bucket) { throw "GCS_BUCKET env var not set" }

$dest = "gs://$bucket/$prefix/date=$LogDate/"
Write-Host "Uploading $LocalDir -> $dest"

& gcloud storage rsync $LocalDir $dest --recursive
if ($LASTEXITCODE -ne 0) { throw "gcloud storage rsync failed with exit code $LASTEXITCODE" }

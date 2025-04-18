param($version,$binary)

If ( ! ( Test-Path Env:wix ) ) {
    Write-Error 'Could not find WiX binaries %wix%'
    exit 1
}

$filename="${binary}_${version}_amd64.msi"
$tarfilename="${binary}_${version}_windows_amd64.tar.gz"
$wixVersion="0.0.0"
$wixVersionMatch=[regex]::Match($version, '^([0-9]+\.[0-9]+\.[0-9]+)')
If ( $wixVersionMatch.success ) {
    $wixVersion=$wixVersionMatch.captures.groups[1].value
} Else {
    Write-Error "Invalid version $version"
    exit 1
}

.\build.ps1 `
  -version $version `
  -binary $binary

tar -cvzf "${tarfilename}" "${binary}.exe"

$appname=(Get-Culture).TextInfo.ToTitleCase($binary)

& "${env:wix}bin\candle.exe" `
  -nologo `
  -arch x64 `
  "-dAppVersion=$version" `
  "-dWixVersion=$wixVersion" `
  "-dAppName=$appname" `
  release.wxs
If ( $LastExitCode -ne 0 ) {
    exit $LastExitCode
}
& "${env:wix}bin\light.exe" `
  -nologo `
  -dcl:high `
  -ext WixUIExtension `
  -ext WixUtilExtension `
  release.wixobj `
  -o $filename
If ( $LastExitCode -ne 0 ) {
    exit $LastExitCode
}

echo file=$filename >> $env:GITHUB_OUTPUT
echo archive=$tarfilename >> $env:GITHUB_OUTPUT
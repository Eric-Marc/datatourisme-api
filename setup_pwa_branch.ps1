# ============================================================================
# 📱 Script PowerShell pour créer la branche feature/pwa sur GitHub
# ============================================================================
# 
# Exécutez ce script DEPUIS LE DOSSIER gedeon-pwa extrait du ZIP
# Usage: .\setup_pwa_branch.ps1 -RepoPath "C:\chemin\vers\datatourisme-api"
#
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$RepoPath
)

Write-Host ""
Write-Host "📱 GEDEON - Configuration PWA" -ForegroundColor Cyan
Write-Host "==============================" -ForegroundColor Cyan
Write-Host ""

# Dossier du script (où sont les fichiers PWA)
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Vérifier que le repo existe
if (-not (Test-Path "$RepoPath\.git")) {
    Write-Host "❌ $RepoPath n'est pas un dépôt Git" -ForegroundColor Red
    exit 1
}

# Vérifier que les fichiers PWA sont présents
if (-not (Test-Path "$ScriptDir\manifest.json")) {
    Write-Host "❌ Fichiers PWA non trouvés. Exécutez ce script depuis le dossier gedeon-pwa\" -ForegroundColor Red
    exit 1
}

Write-Host "📍 Dossier PWA source: $ScriptDir" -ForegroundColor Gray
Write-Host "📍 Dépôt cible: $RepoPath" -ForegroundColor Gray
Write-Host ""

# Aller dans le repo
Set-Location $RepoPath

# Afficher la branche actuelle
$CurrentBranch = git branch --show-current
Write-Host "📍 Branche actuelle: $CurrentBranch" -ForegroundColor Gray

# Mise à jour depuis origin
Write-Host ""
Write-Host "📥 Mise à jour depuis origin..." -ForegroundColor Yellow
git fetch origin

# Créer la nouvelle branche depuis feature/auth-email
Write-Host ""
Write-Host "🌿 Création de la branche feature/pwa..." -ForegroundColor Yellow
git checkout feature/auth-email
git pull origin feature/auth-email
git checkout -b feature/pwa

# Créer le dossier icons
Write-Host ""
Write-Host "📁 Création du dossier icons\..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "icons" | Out-Null

# Copier les fichiers PWA
Write-Host ""
Write-Host "📋 Copie des fichiers PWA..." -ForegroundColor Yellow

Copy-Item "$ScriptDir\manifest.json" -Destination "." -Force
Write-Host "   ✅ manifest.json" -ForegroundColor Green

Copy-Item "$ScriptDir\sw.js" -Destination "." -Force
Write-Host "   ✅ sw.js" -ForegroundColor Green

Copy-Item "$ScriptDir\offline.html" -Destination "." -Force
Write-Host "   ✅ offline.html" -ForegroundColor Green

Copy-Item "$ScriptDir\index.html" -Destination "." -Force
Write-Host "   ✅ index.html (modifié)" -ForegroundColor Green

Copy-Item "$ScriptDir\scanner.html" -Destination "." -Force
Write-Host "   ✅ scanner.html (modifié)" -ForegroundColor Green

Copy-Item "$ScriptDir\icons\*" -Destination ".\icons\" -Force
$IconCount = (Get-ChildItem ".\icons\").Count
Write-Host "   ✅ icons\ ($IconCount fichiers)" -ForegroundColor Green

# Afficher le statut git
Write-Host ""
Write-Host "📊 Fichiers ajoutés:" -ForegroundColor Yellow
git status --short

# Demander confirmation pour commit
Write-Host ""
$Confirm = Read-Host "Voulez-vous commiter et pusher ? (oui/non)"

if ($Confirm -eq "oui" -or $Confirm -eq "OUI" -or $Confirm -eq "o" -or $Confirm -eq "O") {
    git add .
    git commit -m "feat: Add PWA support for mobile app installation

- Add manifest.json for PWA metadata
- Add service worker (sw.js) for offline caching
- Add offline.html fallback page
- Add PWA icons (72px to 512px)
- Update index.html and scanner.html with PWA meta tags
- Enable 'Add to Home Screen' on mobile devices"

    Write-Host ""
    Write-Host "📤 Push vers origin/feature/pwa..." -ForegroundColor Yellow
    git push -u origin feature/pwa
    
    Write-Host ""
    Write-Host "✅ Terminé ! La branche feature/pwa est sur GitHub." -ForegroundColor Green
    Write-Host ""
    Write-Host "🔗 Créez une Pull Request sur GitHub :" -ForegroundColor Cyan
    Write-Host "   https://github.com/Eric-Marc/datatourisme-api/compare/feature/auth-email...feature/pwa" -ForegroundColor White
}
else {
    Write-Host ""
    Write-Host "✅ Fichiers copiés. Pour finaliser manuellement :" -ForegroundColor Green
    Write-Host "   git add ." -ForegroundColor White
    Write-Host "   git commit -m 'feat: Add PWA support'" -ForegroundColor White
    Write-Host "   git push -u origin feature/pwa" -ForegroundColor White
}

Write-Host ""

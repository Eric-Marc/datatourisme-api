#!/bin/bash
# ============================================================================
# 📱 Script pour créer la branche feature/pwa sur GitHub
# ============================================================================
# 
# Exécutez ce script DEPUIS LE DOSSIER gedeon-pwa extrait du ZIP
# Usage: bash setup_pwa_branch.sh /chemin/vers/datatourisme-api
#
# ============================================================================

set -e  # Arrêter en cas d'erreur

echo "📱 GEDEON - Configuration PWA"
echo "=============================="
echo ""

# Vérifier qu'un chemin est fourni
if [ -z "$1" ]; then
    echo "Usage: bash setup_pwa_branch.sh /chemin/vers/datatourisme-api"
    echo ""
    echo "Exemple: bash setup_pwa_branch.sh ~/Desktop/Event/datatourisme-api"
    exit 1
fi

REPO_PATH="$1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Vérifier que le repo existe
if [ ! -d "$REPO_PATH/.git" ]; then
    echo "❌ $REPO_PATH n'est pas un dépôt Git"
    exit 1
fi

# Vérifier que les fichiers PWA sont présents
if [ ! -f "$SCRIPT_DIR/manifest.json" ]; then
    echo "❌ Fichiers PWA non trouvés. Exécutez ce script depuis le dossier gedeon-pwa/"
    exit 1
fi

echo "📍 Dossier PWA source: $SCRIPT_DIR"
echo "📍 Dépôt cible: $REPO_PATH"
echo ""

# Aller dans le repo
cd "$REPO_PATH"

# Sauvegarder la branche actuelle
CURRENT_BRANCH=$(git branch --show-current)
echo "📍 Branche actuelle: $CURRENT_BRANCH"

# S'assurer qu'on est à jour
echo ""
echo "📥 Mise à jour depuis origin..."
git fetch origin

# Créer la nouvelle branche depuis feature/auth-email
echo ""
echo "🌿 Création de la branche feature/pwa..."
git checkout feature/auth-email
git pull origin feature/auth-email
git checkout -b feature/pwa

# Créer le dossier icons
echo ""
echo "📁 Création du dossier icons/..."
mkdir -p icons

# Copier les fichiers PWA
echo ""
echo "📋 Copie des fichiers PWA..."
cp "$SCRIPT_DIR/manifest.json" .
cp "$SCRIPT_DIR/sw.js" .
cp "$SCRIPT_DIR/offline.html" .
cp "$SCRIPT_DIR/index.html" .
cp "$SCRIPT_DIR/scanner.html" .
cp "$SCRIPT_DIR/icons/"* ./icons/

echo "   ✅ manifest.json"
echo "   ✅ sw.js"
echo "   ✅ offline.html"
echo "   ✅ index.html (modifié)"
echo "   ✅ scanner.html (modifié)"
echo "   ✅ icons/ (10 fichiers)"

# Afficher le statut git
echo ""
echo "📊 Fichiers ajoutés:"
git status --short

# Demander confirmation pour commit
echo ""
read -p "Voulez-vous commiter et pusher ? (oui/non): " CONFIRM

if [ "$CONFIRM" = "oui" ] || [ "$CONFIRM" = "OUI" ] || [ "$CONFIRM" = "o" ]; then
    git add .
    git commit -m "feat: Add PWA support for mobile app installation

- Add manifest.json for PWA metadata
- Add service worker (sw.js) for offline caching
- Add offline.html fallback page
- Add PWA icons (72px to 512px)
- Update index.html and scanner.html with PWA meta tags
- Enable 'Add to Home Screen' on mobile devices"

    echo ""
    echo "📤 Push vers origin/feature/pwa..."
    git push -u origin feature/pwa
    
    echo ""
    echo "✅ Terminé ! La branche feature/pwa est sur GitHub."
    echo ""
    echo "🔗 Créez une Pull Request sur GitHub :"
    echo "   https://github.com/Eric-Marc/datatourisme-api/compare/feature/auth-email...feature/pwa"
else
    echo ""
    echo "✅ Fichiers copiés. Pour finaliser manuellement :"
    echo "   git add ."
    echo "   git commit -m 'feat: Add PWA support'"
    echo "   git push -u origin feature/pwa"
fi

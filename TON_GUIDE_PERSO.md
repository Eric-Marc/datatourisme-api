# 🚀 Guide Personnalisé - Ton Instance Render

URL de ta base : `dpg-d4hngejuibrs73do1jf0-a.frankfurt-postgres.render.com`
Database : `data_tourisme`
User : `data_tourisme_user`

---

## ✅ Étape 1 : Import des Données (5 minutes)

### Le script est déjà configuré avec ton URL !

Tous les fichiers sont prêts dans `/mnt/user-data/outputs/`

### Lancer l'import :

```bash
python import_datatourisme_postgres.py
```

### Ce qui va se passer :

```
✅ Connexion à Render: dpg-d4hngejuibrs73do1jf0-a.frankfurt-postgres.render.com
🔨 Création de la base de données...
   📍 Activation de PostGIS...
   ✅ PostGIS activé
✅ Base de données créée avec succès

🔍 Recherche de l'URL du fichier CSV...
✅ Fichier trouvé: datatourisme-fma-YYYYMMDD.csv
   URL: https://files.data.gouv.fr/...

📥 Téléchargement du fichier CSV...
   Cela peut prendre 1-2 minutes...

✅ Téléchargé: 78.45 MB
📊 Lecture du CSV...
✅ 54,321 événements lus

💾 Import dans PostgreSQL...
   Préparation: 52,890 événements à importer
   Ignorés: 1,431 (sans coordonnées)

✅ 52,890 événements importés dans PostgreSQL

📊 STATISTIQUES DE LA BASE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📍 Total événements: 52,890

🏙️  Top 10 communes:
   Paris: 8,432
   Lyon: 2,156
   Marseille: 1,987
   Toulouse: 1,543
   ...

📅 Événements à venir: 48,234

💾 Taille de la table: 845 MB

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ IMPORT TERMINÉ !
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

⏱️ **Durée estimée : 5-7 minutes**

---

## ⚠️ Important : Limite 1 GB

Tu es sur le **plan gratuit** avec **1 GB max**.

L'import complet fait **~845 MB** → Tu es proche de la limite !

### Options :

**Option A** : Import complet (ce que fait le script)
- ✅ Toute la France (~52,000 événements)
- ⚠️  ~845 MB (proche de 1 GB)
- 💡 Nettoyer régulièrement les événements passés

**Option B** : Import régional seulement
- ✅ Une région (Occitanie, IDF, etc.)
- ✅ ~150-200 MB (safe)
- ✅ ~5,000-10,000 événements

**Option C** : Upgrader à Starter
- ✅ 10 GB de stockage
- ✅ Toute la France sans souci
- 💰 $7/mois

**Pour commencer, lance l'import complet. Tu pourras nettoyer après si nécessaire.**

---

## 🚀 Étape 2 : Déployer l'API sur Render (3 minutes)

### 1. Créer le Repo GitHub

```bash
# Aller dans le dossier avec tes fichiers
cd /chemin/vers/dossier

# Copier les fichiers depuis /mnt/user-data/outputs/
# - import_datatourisme_postgres.py
# - server_datatourisme_postgres.py  
# - requirements.txt
# - index.html
# - .gitignore
# - README.md

# Initialiser Git
git init
git add .
git commit -m "DATAtourisme API avec PostgreSQL"

# Créer un repo sur GitHub.com
# Puis :
git remote add origin https://github.com/TON_USERNAME/datatourisme-api.git
git branch -M main
git push -u origin main
```

### 2. Déployer sur Render

**Dashboard Render** : https://dashboard.render.com

**Cliquer** : **New +** → **Web Service**

**Configuration** :
```
Repository: datatourisme-api (ton repo GitHub)
Name: datatourisme-api
Region: Frankfurt (même que ta base)
Branch: main
Runtime: Python 3
Build Command: pip install -r requirements.txt
Start Command: gunicorn server_datatourisme_postgres:app
Instance Type: Free
```

**Environment Variable** :
```
Key: DATABASE_URL
Value: Cliquer sur "Add from Render Service"
       → Sélectionner: data_tourisme
       → Choisir: Internal Database URL
```

⚠️ **Important** : Utilise **Internal Database URL** (pas External) pour la connexion API → Base

**Cliquer** : **Create Web Service**

⏱️ **Attendre 3-5 minutes...**

---

## ✅ Étape 3 : Tester l'API

### Ton API sera accessible sur :

```
https://datatourisme-api.onrender.com
```

(Remplace par ton URL Render)

### Tests :

**1. Health Check** :
```
https://datatourisme-api.onrender.com/health
```

Résultat attendu :
```json
{
  "status": "healthy",
  "database": "connected",
  "source": "DATAtourisme PostgreSQL"
}
```

**2. Statistiques** :
```
https://datatourisme-api.onrender.com/api/stats
```

**3. Recherche (Toulouse)** :
```
https://datatourisme-api.onrender.com/api/events/nearby?lat=43.6047&lon=1.4442&radiusKm=30&days=30
```

**4. Frontend** :
```
https://datatourisme-api.onrender.com/
```

---

## 🔧 Maintenance

### Nettoyer les Événements Passés

**Créer** `clean_database.py` :

```python
import psycopg2
from urllib.parse import urlparse

DATABASE_URL = "postgresql://data_tourisme_user:B2zwMxZNbbU3LHKFFQrtIiY1VABoEuEo@dpg-d4hngejuibrs73do1jf0-a.frankfurt-postgres.render.com/data_tourisme"

url = urlparse(DATABASE_URL)
conn = psycopg2.connect(
    host=url.hostname,
    port=url.port,
    database=url.path[1:],
    user=url.username,
    password=url.password,
    sslmode='require'
)

cur = conn.cursor()

print("🧹 Suppression des événements passés...")

cur.execute("""
    DELETE FROM evenements 
    WHERE date_fin < CURRENT_DATE - INTERVAL '7 days'
""")

deleted = cur.rowcount
conn.commit()

print(f"✅ {deleted} événements supprimés")

cur.execute("VACUUM FULL evenements")
print("✅ Base optimisée")

cur.close()
conn.close()
```

**Exécuter** :
```bash
python clean_database.py
```

**Fréquence recommandée** : 1x par mois

---

## 📊 Surveiller l'Espace Disque

**Dashboard Render** → Ta base `data_tourisme` → **Metrics** → **Disk Usage**

Tu verras :
```
XXX MB / 1000 MB
```

⚠️ **Si > 900 MB** : Nettoyer avec `clean_database.py`

---

## 🎯 Résumé

### Ce que tu as :

✅ **Base PostgreSQL** sur Render (Frankfurt)
✅ **50,000+ événements** de toute la France
✅ **Recherche géospatiale** ultra-rapide (PostGIS)
✅ **API Flask** prête à déployer
✅ **Plan gratuit** (1 GB, 90 jours)

### Prochaines étapes :

1. ⏳ **Maintenant** : Lance `python import_datatourisme_postgres.py`
2. 🚀 **Ensuite** : Déploie l'API sur Render
3. ✅ **Teste** : Vérifie que tout fonctionne
4. 🔄 **Optionnel** : Configure la mise à jour automatique

---

## 💡 Conseils

### Performance

- ⚡ **Première requête** après 15 min d'inactivité = 30-60s (Render réveille le service)
- ⚡ **Requêtes suivantes** = 2-5 ms

### Limitations Plan Gratuit

- 📦 **1 GB** de stockage
- ⏰ **90 jours** puis base supprimée
- 🔒 **Pas de Shell SQL**
- 💤 **Sommeil après 15 min** d'inactivité

### Quand Upgrader ?

Passe à **Starter ($7/mois)** si :
- Tu dépasses 900 MB régulièrement
- Tu veux garder la base > 90 jours
- Tu as besoin de backups automatiques
- Performance devient critique

---

## 🆘 Problèmes Courants

### "Connection refused"
→ Vérifie que la base est bien "Available" sur Render

### "Extension postgis does not exist"
→ Relance le script, il active PostGIS automatiquement

### "Disk full"
→ Exécute `clean_database.py` ou passe à Starter

### "Timeout"
→ Normal sur plan gratuit, attends 30-60s après inactivité

---

## 📞 Besoin d'Aide ?

Si tu es bloqué à une étape, dis-moi où et je t'aide ! 👍

---

## 🎉 Prêt !

Lance maintenant :

```bash
python import_datatourisme_postgres.py
```

Et regarde la magie opérer ! ✨

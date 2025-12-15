# 🇫🇷 DATAtourisme API Nationale

API complète pour accéder aux événements culturels et touristiques de toute la France via DATAtourisme.

## ✨ Fonctionnalités

- 🗺️ **50,000+ événements** en France
- ⚡ **Recherche ultra-rapide** (2-5 ms)
- 📍 **Recherche géographique** par rayon
- 📅 **Filtrage temporel** configurable
- 🌐 **API REST** simple et rapide
- 💾 **PostgreSQL + PostGIS** pour performances optimales

## 🚀 Déploiement sur Render

### 1. PostgreSQL

```
New → PostgreSQL
Name: datatourisme-db
Region: Frankfurt
Plan: Starter ($7/mois)
```

Activer PostGIS dans Shell :
```sql
CREATE EXTENSION postgis;
```

### 2. Importer les Données

```bash
# Configurer DATABASE_URL avec External URL de Render
export DATABASE_URL="postgres://user:pass@host.render.com/datatourisme"

# Importer
python import_datatourisme_postgres.py
```

### 3. Déployer l'API

```
New → Web Service
Repository: Ce repo GitHub
Build: pip install -r requirements.txt
Start: gunicorn server_datatourisme_postgres:app
Environment Variable: DATABASE_URL (Internal Connection String)
```

## 📡 Endpoints

### Health Check
```
GET /health
```

### Statistiques
```
GET /api/stats
```

### Recherche d'Événements
```
GET /api/events/nearby?lat=43.6047&lon=1.4442&radiusKm=30&days=30
```

**Paramètres :**
- `lat` (float, requis) : Latitude du centre
- `lon` (float, requis) : Longitude du centre
- `radiusKm` (int, optionnel) : Rayon en km (défaut: 30)
- `days` (int, optionnel) : Nombre de jours (défaut: 30)

## 🛠️ Développement Local

### Installation

```bash
# Cloner le repo
git clone https://github.com/TON_USERNAME/datatourisme-api.git
cd datatourisme-api

# Installer les dépendances
pip install -r requirements.txt

# Configurer PostgreSQL local
createdb datatourisme
psql datatourisme -c "CREATE EXTENSION postgis;"

# Importer les données
python import_datatourisme_postgres.py

# Lancer l'API
python server_datatourisme_postgres.py
```

L'API sera disponible sur http://localhost:5000

## 💰 Coûts Render

- PostgreSQL Starter : $7/mois
- API Flask Free : $0
- **Total : $7/mois**

## 📄 Licence

MIT - Données DATAtourisme sous Licence Ouverte 2.0

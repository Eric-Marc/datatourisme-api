#!/usr/bin/env python3
"""
Import des données DATAtourisme (CSV) vers PostgreSQL
Solution nationale complète avec géolocalisation
"""

import requests
import csv
from io import StringIO
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
from math import radians, sin, cos, sqrt, atan2
import json


# ============================================================================
# CONFIGURATION
# ============================================================================

import os
from urllib.parse import urlparse

# URL de ta base PostgreSQL sur Render
DATABASE_URL = os.environ.get('DATABASE_URL') or "postgresql://data_tourisme_user:B2zwMxZNbbU3LHKFFQrtIiY1VABoEuEo@dpg-d4hngejuibrs73do1jf0-a.frankfurt-postgres.render.com/data_tourisme"

if DATABASE_URL:
    # Parser l'URL de Render
    url = urlparse(DATABASE_URL)
    DB_CONFIG = {
        'host': url.hostname,
        'port': url.port or 5432,
        'database': url.path[1:],  # Enlever le / initial
        'user': url.username,
        'password': url.password,
        'sslmode': 'require'  # Important pour Render
    }
    print(f"✅ Connexion à Render: {url.hostname}")
else:
    # Configuration locale (ne sera pas utilisée)
    DB_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'datatourisme',
        'user': 'postgres',
        'password': 'votre_mot_de_passe',
        'sslmode': 'prefer'
    }
    print(f"⚠️  Connexion locale: {DB_CONFIG['host']}")

# DATAtourisme
DATASET_ID = "5b598be088ee387c0c353714"
DATA_GOUV_API = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ID}/"


# ============================================================================
# FONCTIONS BASE DE DONNÉES
# ============================================================================

def creer_base_donnees():
    """Crée la structure de la base de données"""
    
    print("🔨 Création de la base de données...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True  # Important pour CREATE EXTENSION
    cur = conn.cursor()
    
    try:
        # Activer l'extension PostGIS pour la géolocalisation
        print("   📍 Activation de PostGIS...")
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        print("   ✅ PostGIS activé")
    except Exception as e:
        print(f"   ⚠️  PostGIS: {e}")
        print("   💡 Si erreur de permission, PostGIS est peut-être déjà activé")
    
    conn.autocommit = False  # Revenir au mode transaction
    
    # Table des événements
    cur.execute("""
        DROP TABLE IF EXISTS evenements CASCADE;
        
        CREATE TABLE evenements (
            id SERIAL PRIMARY KEY,
            uri TEXT UNIQUE,
            nom TEXT NOT NULL,
            categories TEXT[],
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            geom GEOMETRY(Point, 4326),  -- PostGIS
            adresse TEXT,
            code_postal TEXT,
            commune TEXT,
            periodes TEXT,
            date_debut DATE,
            date_fin DATE,
            contacts TEXT,
            classements TEXT,
            description TEXT,
            mesures_covid TEXT,
            createur TEXT,
            sit_diffuseur TEXT,
            date_maj TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        -- Index géospatiaux
        CREATE INDEX idx_evenements_geom ON evenements USING GIST(geom);
        CREATE INDEX idx_evenements_dates ON evenements(date_debut, date_fin);
        CREATE INDEX idx_evenements_commune ON evenements(commune);
        CREATE INDEX idx_evenements_code_postal ON evenements(code_postal);
        CREATE INDEX idx_evenements_categories ON evenements USING GIN(categories);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("✅ Base de données créée avec succès")


def trouver_url_csv_evenements():
    """Trouve l'URL du fichier CSV événements via l'API data.gouv.fr"""
    
    print("🔍 Recherche de l'URL du fichier CSV...")
    
    try:
        response = requests.get(DATA_GOUV_API, timeout=30)
        response.raise_for_status()
        
        dataset = response.json()
        resources = dataset.get('resources', [])
        
        for resource in resources:
            title = resource.get('title', '').lower()
            format_type = resource.get('format', '').lower()
            
            # Chercher le fichier FMA (événements)
            if format_type == 'csv' and ('fma' in title or 'événement' in title or 'manifestation' in title):
                url = resource.get('url')
                print(f"✅ Fichier trouvé: {resource.get('title')}")
                print(f"   URL: {url}\n")
                return url
        
        print("❌ Aucun fichier CSV d'événements trouvé")
        return None
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return None


def telecharger_et_importer_csv(url):
    """Télécharge le CSV et l'importe dans PostgreSQL"""
    
    if not url:
        return 0
    
    print(f"📥 Téléchargement du fichier CSV...")
    print(f"   Cela peut prendre 1-2 minutes...\n")
    
    try:
        response = requests.get(url, timeout=300)  # 5 minutes timeout
        response.raise_for_status()
        
        size_mb = len(response.content) / (1024 * 1024)
        print(f"✅ Téléchargé: {size_mb:.2f} MB")
        
        print("📊 Lecture du CSV...")
        content = response.content.decode('utf-8', errors='ignore')
        csv_file = StringIO(content)
        reader = csv.DictReader(csv_file)
        
        evenements = list(reader)
        print(f"✅ {len(evenements)} événements lus\n")
        
        # Import dans PostgreSQL
        print("💾 Import dans PostgreSQL...")
        count = importer_evenements_postgres(evenements)
        
        return count
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return 0


def parser_periodes(periodes_str):
    """Parse les périodes et retourne la première date de début et fin"""
    
    if not periodes_str:
        return None, None
    
    try:
        # Format: 2024-06-15<->2024-06-20|2024-07-01<->2024-07-10
        periodes = periodes_str.split('|')
        if periodes:
            premiere = periodes[0].split('<->')
            if len(premiere) == 2:
                debut = datetime.strptime(premiere[0].strip(), '%Y-%m-%d').date()
                fin = datetime.strptime(premiere[1].strip(), '%Y-%m-%d').date()
                return debut, fin
    except:
        pass
    
    return None, None


def parser_categories(categories_str):
    """Parse les catégories en tableau"""
    
    if not categories_str:
        return []
    
    # Format: https://url1|https://url2
    categories = categories_str.split('|')
    
    # Extraire seulement le dernier segment de l'URL
    result = []
    for cat in categories:
        if '/' in cat:
            result.append(cat.split('/')[-1])
        else:
            result.append(cat)
    
    return result


def parser_code_postal_commune(cp_commune_str):
    """Parse le format code_postal#commune"""
    
    if not cp_commune_str:
        return None, None
    
    try:
        parts = cp_commune_str.split('#')
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
    except:
        pass
    
    return None, None


def importer_evenements_postgres(evenements):
    """Importe les événements dans PostgreSQL par batch"""
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Préparer les données
    data_to_insert = []
    skipped = 0
    
    for event in evenements:
        try:
            # Coordonnées GPS
            lat = event.get('Latitude')
            lon = event.get('Longitude')
            
            if not lat or not lon:
                skipped += 1
                continue
            
            lat = float(lat)
            lon = float(lon)
            
            # Parser les périodes
            periodes = event.get('Periodes_regroupees', '')
            date_debut, date_fin = parser_periodes(periodes)
            
            # Parser catégories
            categories = parser_categories(event.get('Categories_de_POI', ''))
            
            # Parser code postal et commune
            cp, commune = parser_code_postal_commune(event.get('Code_postal_et_commune', ''))
            
            # Date de mise à jour
            date_maj_str = event.get('Date_de_mise_a_jour', '')
            try:
                date_maj = datetime.fromisoformat(date_maj_str.replace('Z', '+00:00'))
            except:
                date_maj = None
            
            data_to_insert.append((
                event.get('URI_ID_du_POI', ''),
                event.get('Nom_du_POI', 'Sans nom'),
                categories,
                lat,
                lon,
                f'POINT({lon} {lat})',  # PostGIS
                event.get('Adresse_postale', ''),
                cp,
                commune,
                periodes,
                date_debut,
                date_fin,
                event.get('Contacts_du_POI', ''),
                event.get('Classements_du_POI', ''),
                event.get('Description', ''),
                event.get('Covid19_mesures_specifiques', ''),
                event.get('Createur_de_la_donnee', ''),
                event.get('SIT_diffuseur', ''),
                date_maj
            ))
            
        except Exception as e:
            skipped += 1
            continue
    
    print(f"   Préparation: {len(data_to_insert)} événements à importer")
    print(f"   Ignorés: {skipped} (sans coordonnées)\n")
    
    # Import par batch (plus rapide)
    query = """
        INSERT INTO evenements (
            uri, nom, categories, latitude, longitude, geom,
            adresse, code_postal, commune, periodes,
            date_debut, date_fin, contacts, classements,
            description, mesures_covid, createur, sit_diffuseur, date_maj
        ) VALUES (
            %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326),
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (uri) DO UPDATE SET
            nom = EXCLUDED.nom,
            categories = EXCLUDED.categories,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            geom = EXCLUDED.geom,
            adresse = EXCLUDED.adresse,
            code_postal = EXCLUDED.code_postal,
            commune = EXCLUDED.commune,
            periodes = EXCLUDED.periodes,
            date_debut = EXCLUDED.date_debut,
            date_fin = EXCLUDED.date_fin,
            contacts = EXCLUDED.contacts,
            classements = EXCLUDED.classements,
            description = EXCLUDED.description,
            date_maj = EXCLUDED.date_maj
    """
    
    execute_batch(cur, query, data_to_insert, page_size=1000)
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"✅ {len(data_to_insert)} événements importés dans PostgreSQL")
    
    return len(data_to_insert)


def afficher_statistiques():
    """Affiche les statistiques de la base"""
    
    print("\n" + "="*70)
    print("📊 STATISTIQUES DE LA BASE")
    print("="*70 + "\n")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Total événements
    cur.execute("SELECT COUNT(*) FROM evenements")
    total = cur.fetchone()[0]
    print(f"📍 Total événements: {total:,}")
    
    # Par région (top 10)
    cur.execute("""
        SELECT commune, COUNT(*) as count
        FROM evenements
        WHERE commune IS NOT NULL
        GROUP BY commune
        ORDER BY count DESC
        LIMIT 10
    """)
    print("\n🏙️  Top 10 communes:")
    for commune, count in cur.fetchall():
        print(f"   {commune}: {count:,}")
    
    # Événements à venir
    cur.execute("""
        SELECT COUNT(*)
        FROM evenements
        WHERE date_debut >= CURRENT_DATE
    """)
    futurs = cur.fetchone()[0]
    print(f"\n📅 Événements à venir: {futurs:,}")
    
    # Taille de la base
    cur.execute("""
        SELECT pg_size_pretty(pg_total_relation_size('evenements'))
    """)
    size = cur.fetchone()[0]
    print(f"\n💾 Taille de la table: {size}")
    
    cur.close()
    conn.close()


# ============================================================================
# FONCTION DE RECHERCHE
# ============================================================================

def rechercher_evenements(latitude, longitude, rayon_km, jours_avant=30):
    """Recherche des événements dans PostgreSQL"""
    
    print(f"\n🔍 Recherche d'événements...")
    print(f"   Centre: ({latitude}, {longitude})")
    print(f"   Rayon: {rayon_km} km")
    print(f"   Période: {jours_avant} jours\n")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    date_limite = datetime.now().date() + timedelta(days=jours_avant)
    
    # Requête avec PostGIS
    query = """
        SELECT 
            nom,
            commune,
            date_debut,
            ST_Distance(
                geom::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            ) / 1000 as distance_km
        FROM evenements
        WHERE ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
            %s  -- rayon en mètres
        )
        AND (date_debut IS NULL OR date_debut <= %s)
        AND (date_fin IS NULL OR date_fin >= CURRENT_DATE)
        ORDER BY distance_km, date_debut
        LIMIT 50
    """
    
    cur.execute(query, (
        longitude, latitude,  # Pour le calcul de distance
        longitude, latitude,  # Pour le filtre géographique
        rayon_km * 1000,      # Rayon en mètres
        date_limite
    ))
    
    results = cur.fetchall()
    
    print(f"✅ {len(results)} événements trouvés\n")
    
    for i, (nom, commune, date, distance) in enumerate(results[:10], 1):
        date_str = date.strftime('%d/%m/%Y') if date else 'Date non précisée'
        print(f"{i}. {nom}")
        print(f"   📍 {commune} ({distance:.1f} km)")
        print(f"   📅 {date_str}\n")
    
    cur.close()
    conn.close()
    
    return results


# ============================================================================
# PROGRAMME PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    
    print("="*70)
    print("🎯 DATATOURISME → POSTGRESQL")
    print("="*70)
    print()
    
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'search':
        # Mode recherche
        rechercher_evenements(43.6047, 1.4442, 30, 30)
    
    else:
        # Mode import
        print("1️⃣  Création de la structure...")
        creer_base_donnees()
        print()
        
        print("2️⃣  Recherche du fichier CSV...")
        url = trouver_url_csv_evenements()
        print()
        
        if url:
            print("3️⃣  Téléchargement et import...")
            count = telecharger_et_importer_csv(url)
            print()
            
            if count > 0:
                print("4️⃣  Statistiques...")
                afficher_statistiques()
                print()
                
                print("="*70)
                print("✅ IMPORT TERMINÉ !")
                print("="*70)
                print()
                print("💡 Pour rechercher des événements:")
                print("   python import_datatourisme.py search")
                print()
        else:
            print("❌ Impossible de trouver le fichier CSV")

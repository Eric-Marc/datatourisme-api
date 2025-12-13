#!/usr/bin/env python3
"""
Script pour compléter les cinémas manquants

Usage:
    python complete_cinemas.py

1. Charge cinemas_allocine_complet.json
2. Récupère tous les cinémas de tous les départements via API Allociné
3. Ajoute les manquants avec géocodage data.gouv.fr
4. Sauvegarde le fichier mis à jour
"""

import json
import csv
import io
import re
import requests
import time

try:
    from allocineAPI.allocineAPI import allocineAPI
    ALLOCINE_OK = True
except ImportError:
    ALLOCINE_OK = False
    print("❌ allocineAPI non disponible")


def geocode_batch_datagouv(addresses):
    """Géocode un lot d'adresses avec l'API Adresse data.gouv.fr"""
    if not addresses:
        return {}
    
    csv_content = "id,adresse\n"
    for addr_id, addr in addresses:
        addr_clean = addr.replace('"', '').replace('\n', ' ').strip()
        csv_content += f'"{addr_id}","{addr_clean}"\n'
    
    url = "https://api-adresse.data.gouv.fr/search/csv/"
    
    try:
        files = {'data': ('addresses.csv', csv_content, 'text/csv')}
        params = {'columns': 'adresse'}
        
        response = requests.post(url, files=files, params=params, timeout=60)
        response.raise_for_status()
        
        results = {}
        reader = csv.DictReader(io.StringIO(response.text))
        
        for row in reader:
            addr_id = row.get('id', '')
            lat = row.get('latitude', '')
            lon = row.get('longitude', '')
            score = row.get('result_score', '0')
            
            if lat and lon and float(score) > 0.3:
                results[addr_id] = (float(lat), float(lon))
            else:
                results[addr_id] = None
        
        return results
    except Exception as e:
        print(f"   ⚠️ Erreur géocodage: {e}")
        return {}


def extract_dept_from_address(address):
    """Extrait le code département depuis l'adresse"""
    if not address:
        return None
    match = re.search(r'(\d{5})', address)
    if match:
        cp = match.group(1)
        if cp.startswith('20'):
            return '2A' if int(cp) < 20200 else '2B'
        return cp[:2]
    return None


def main():
    if not ALLOCINE_OK:
        return
    
    print("=" * 70)
    print("🎬 COMPLÉTION DES CINÉMAS ALLOCINÉ")
    print("=" * 70)
    
    # 1. Charger le fichier existant
    print("\n📂 Chargement du fichier existant...")
    try:
        with open('cinemas_allocine_complet.json', 'r', encoding='utf-8') as f:
            existing = json.load(f)
        print(f"   {len(existing)} cinémas existants")
    except FileNotFoundError:
        existing = []
        print("   Fichier non trouvé, création depuis zéro")
    
    existing_ids = {c['id'] for c in existing}
    
    # 2. Récupérer TOUS les cinémas de TOUS les départements
    print("\n🔍 Récupération de tous les cinémas Allociné...")
    api = allocineAPI()
    
    all_cinemas = []
    depts = api.get_departements()
    print(f"   {len(depts)} départements trouvés")
    
    for i, dept in enumerate(depts):
        dept_name = dept.get('name', '?')
        dept_id = dept.get('id')
        
        try:
            cinemas = api.get_cinema(dept_id)
            all_cinemas.extend(cinemas)
            print(f"   [{i+1}/{len(depts)}] {dept_name}: {len(cinemas)} cinémas")
            time.sleep(0.2)  # Rate limiting
        except Exception as e:
            print(f"   [{i+1}/{len(depts)}] {dept_name}: ❌ {e}")
    
    print(f"\n   Total récupéré: {len(all_cinemas)} cinémas")
    
    # 3. Trouver les cinémas manquants
    all_ids = {c['id'] for c in all_cinemas}
    missing_ids = all_ids - existing_ids
    new_ids = existing_ids - all_ids
    
    print(f"\n📊 Comparaison:")
    print(f"   Dans le fichier: {len(existing_ids)}")
    print(f"   Depuis l'API: {len(all_ids)}")
    print(f"   Manquants: {len(missing_ids)}")
    print(f"   Obsolètes: {len(new_ids)}")
    
    if not missing_ids:
        print("\n✅ Aucun cinéma manquant!")
        return
    
    # 4. Préparer les cinémas manquants
    missing_cinemas = [c for c in all_cinemas if c['id'] in missing_ids]
    
    print(f"\n🆕 Cinémas à ajouter: {len(missing_cinemas)}")
    for c in missing_cinemas[:10]:
        print(f"   {c['id']}: {c['name'][:40]}")
    if len(missing_cinemas) > 10:
        print(f"   ... et {len(missing_cinemas) - 10} autres")
    
    # 5. Géocoder les nouveaux cinémas
    print(f"\n📍 Géocodage des {len(missing_cinemas)} cinémas...")
    
    to_geocode = []
    for c in missing_cinemas:
        addr = c.get('address', '')
        if addr:
            to_geocode.append((c['id'], addr + ', France'))
    
    geocoded = {}
    BATCH_SIZE = 500
    
    for i in range(0, len(to_geocode), BATCH_SIZE):
        batch = to_geocode[i:i+BATCH_SIZE]
        print(f"   Lot {i//BATCH_SIZE + 1}: {len(batch)} adresses...")
        results = geocode_batch_datagouv(batch)
        geocoded.update(results)
        success = sum(1 for v in results.values() if v)
        print(f"      ✅ {success}/{len(batch)} géocodés")
    
    # 6. Créer les entrées pour les nouveaux cinémas
    new_entries = []
    for c in missing_cinemas:
        cid = c['id']
        coords = geocoded.get(cid)
        
        entry = {
            'id': cid,
            'name': c.get('name', ''),
            'address': c.get('address', ''),
            'dept': extract_dept_from_address(c.get('address', '')),
            'lat': coords[0] if coords else None,
            'lon': coords[1] if coords else None,
            'source': 'DataGouv' if coords else None
        }
        new_entries.append(entry)
    
    # 7. Fusionner avec l'existant
    final = existing + new_entries
    
    with_gps = len([c for c in final if c.get('lat')])
    print(f"\n📊 Résultat final:")
    print(f"   Total: {len(final)} cinémas")
    print(f"   Avec GPS: {with_gps} ({100*with_gps/len(final):.1f}%)")
    
    # 8. Sauvegarder
    print("\n💾 Sauvegarde...")
    with open('cinemas_allocine_complet.json', 'w', encoding='utf-8') as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    print("   ✅ cinemas_allocine_complet.json")
    
    # Aussi sauvegarder sous le nom utilisé par le serveur
    with open('cinemas_france_data.json', 'w', encoding='utf-8') as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    print("   ✅ cinemas_france_data.json")
    
    print("\n✅ Terminé!")


if __name__ == '__main__':
    main()
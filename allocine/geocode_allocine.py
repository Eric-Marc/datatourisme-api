#!/usr/bin/env python3
"""
Script pour REGÉOCODER tous les cinémas Allociné

Le fichier existant a des coordonnées GPS incorrectes.
Ce script récupère tous les cinémas et les géocode avec data.gouv.fr

Usage:
    python regeocode_cinemas.py
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
        print("❌ Impossible sans allocineAPI")
        return
    
    print("=" * 70)
    print("🎬 REGÉOCODAGE COMPLET DES CINÉMAS ALLOCINÉ")
    print("=" * 70)
    
    # 1. Récupérer TOUS les cinémas de TOUS les départements
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
            for c in cinemas:
                c['dept_name'] = dept_name
            all_cinemas.extend(cinemas)
            print(f"   [{i+1}/{len(depts)}] {dept_name}: {len(cinemas)} cinémas")
            time.sleep(0.2)
        except Exception as e:
            print(f"   [{i+1}/{len(depts)}] {dept_name}: ❌ {e}")
    
    print(f"\n   Total: {len(all_cinemas)} cinémas")
    
    # 2. Préparer les adresses pour géocodage
    print(f"\n📍 Géocodage de {len(all_cinemas)} cinémas avec data.gouv.fr...")
    
    to_geocode = []
    for c in all_cinemas:
        addr = c.get('address', '')
        if addr:
            to_geocode.append((c['id'], addr + ', France'))
        else:
            to_geocode.append((c['id'], c.get('name', '') + ', France'))
    
    # 3. Géocoder par lots
    geocoded = {}
    BATCH_SIZE = 500
    
    for i in range(0, len(to_geocode), BATCH_SIZE):
        batch = to_geocode[i:i+BATCH_SIZE]
        batch_num = i//BATCH_SIZE + 1
        total_batches = (len(to_geocode)-1)//BATCH_SIZE + 1
        print(f"   Lot {batch_num}/{total_batches}: {len(batch)} adresses...")
        
        results = geocode_batch_datagouv(batch)
        geocoded.update(results)
        
        success = sum(1 for v in results.values() if v)
        print(f"      ✅ {success}/{len(batch)} géocodés")
    
    # 4. Créer le fichier final
    print(f"\n💾 Création du fichier...")
    
    final_cinemas = []
    success_count = 0
    
    for c in all_cinemas:
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
        final_cinemas.append(entry)
        
        if coords:
            success_count += 1
    
    print(f"\n📊 Résultat:")
    print(f"   Total: {len(final_cinemas)} cinémas")
    print(f"   Avec GPS: {success_count} ({100*success_count/len(final_cinemas):.1f}%)")
    print(f"   Sans GPS: {len(final_cinemas) - success_count}")
    
    # 5. Sauvegarder
    with open('cinemas_france_data.json', 'w', encoding='utf-8') as f:
        json.dump(final_cinemas, f, ensure_ascii=False, indent=2)
    print(f"\n✅ Sauvegardé: cinemas_france_data.json")
    
    # 6. Vérification Rennes
    print(f"\n🔍 Vérification (cinémas près de Rennes):")
    import math
    def haversine_km(lat1, lon1, lat2, lon2):
        R = 6371.0
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    center_lat, center_lon = 48.0901, -1.6479  # Rennes
    nearby = []
    for c in final_cinemas:
        if c.get('lat') and c.get('lon'):
            dist = haversine_km(center_lat, center_lon, c['lat'], c['lon'])
            if dist <= 30:
                nearby.append((c['name'], dist))
    
    print(f"   Cinémas dans 30km de Rennes: {len(nearby)}")
    for name, dist in sorted(nearby, key=lambda x: x[1])[:5]:
        print(f"      - {name[:40]}: {dist:.1f}km")


if __name__ == '__main__':
    main()
// GEDEON Service Worker v1.1 - Carte Leaflet
const CACHE_NAME = 'gedeon-cache-v1.1';
const OFFLINE_URL = '/offline.html';

// Fichiers à mettre en cache pour le mode offline
const STATIC_ASSETS = [
  '/',
  '/index.html',
  '/scanner.html',
  '/offline.html',
  '/manifest.json',
  '/icons/icon-192x192.png',
  '/icons/icon-512x512.png',
  'https://cdn.tailwindcss.com',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
  'https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css',
  'https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css',
  'https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js'
];

// Installation du Service Worker
self.addEventListener('install', (event) => {
  console.log('📦 Service Worker: Installation...');
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => {
        console.log('📦 Service Worker: Mise en cache des assets statiques');
        return cache.addAll(STATIC_ASSETS);
      })
      .then(() => self.skipWaiting())
  );
});

// Activation et nettoyage des anciens caches
self.addEventListener('activate', (event) => {
  console.log('✅ Service Worker: Activé');
  event.waitUntil(
    caches.keys().then((cacheNames) => {
      return Promise.all(
        cacheNames.map((cacheName) => {
          if (cacheName !== CACHE_NAME) {
            console.log('🗑️ Service Worker: Suppression ancien cache', cacheName);
            return caches.delete(cacheName);
          }
        })
      );
    }).then(() => self.clients.claim())
  );
});

// Stratégie de fetch : Network First, puis Cache
self.addEventListener('fetch', (event) => {
  const { request } = event;
  const url = new URL(request.url);

  // Ignorer les requêtes non-GET
  if (request.method !== 'GET') {
    return;
  }

  // Pour les requêtes API, toujours aller au réseau
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(
      fetch(request)
        .catch(() => {
          return new Response(
            JSON.stringify({ status: 'error', message: 'Hors ligne' }),
            { headers: { 'Content-Type': 'application/json' } }
          );
        })
    );
    return;
  }

  // Pour les autres ressources : Network First avec fallback Cache
  event.respondWith(
    fetch(request)
      .then((response) => {
        // Mettre en cache la nouvelle réponse
        if (response.status === 200) {
          const responseClone = response.clone();
          caches.open(CACHE_NAME).then((cache) => {
            cache.put(request, responseClone);
          });
        }
        return response;
      })
      .catch(() => {
        // En cas d'échec réseau, utiliser le cache
        return caches.match(request)
          .then((cachedResponse) => {
            if (cachedResponse) {
              return cachedResponse;
            }
            // Si pas de cache et navigation, afficher page offline
            if (request.mode === 'navigate') {
              return caches.match(OFFLINE_URL);
            }
            return new Response('Ressource non disponible hors ligne', {
              status: 503,
              statusText: 'Service Unavailable'
            });
          });
      })
  );
});

// Gestion des notifications push (pour plus tard)
self.addEventListener('push', (event) => {
  if (event.data) {
    const data = event.data.json();
    const options = {
      body: data.body || 'Nouvel événement près de vous !',
      icon: '/icons/icon-192x192.png',
      badge: '/icons/icon-72x72.png',
      vibrate: [100, 50, 100],
      data: {
        url: data.url || '/'
      }
    };
    event.waitUntil(
      self.registration.showNotification(data.title || 'GEDEON', options)
    );
  }
});

// Clic sur notification
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(
    clients.openWindow(event.notification.data.url || '/')
  );
});

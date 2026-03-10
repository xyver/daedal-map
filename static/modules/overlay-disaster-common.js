export function addGenericExitButton(id, text, color, onExit) {
  document.getElementById(id)?.remove();
  const btn = document.createElement('button');
  btn.id = id;
  btn.textContent = text;
  btn.style.cssText = `
    position: fixed; top: 80px; left: 50%; transform: translateX(-50%);
    padding: 10px 20px; background: ${color}; color: white; border: none;
    border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 500;
    z-index: 1000; box-shadow: 0 2px 8px rgba(0,0,0,0.3);
  `;
  btn.addEventListener('click', onExit);
  document.body.appendChild(btn);
}

export function getBoundsFromCoords(coords) {
  if (!coords || coords.length === 0) return null;
  let minLng = Infinity;
  let maxLng = -Infinity;
  let minLat = Infinity;
  let maxLat = -Infinity;
  for (const [lng, lat] of coords) {
    if (lng < minLng) minLng = lng;
    if (lng > maxLng) maxLng = lng;
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
  }
  return [[minLng, minLat], [maxLng, maxLat]];
}

export function createCircleFeature(centerLon, centerLat, radiusKm, steps = 64) {
  const coords = [];
  for (let i = 0; i <= steps; i++) {
    const angle = (i / steps) * 2 * Math.PI;
    const latOffset = (radiusKm / 111) * Math.cos(angle);
    const lonOffset = (radiusKm / (111 * Math.cos(centerLat * Math.PI / 180))) * Math.sin(angle);
    coords.push([centerLon + lonOffset, centerLat + latOffset]);
  }
  return {
    type: 'Feature',
    properties: {},
    geometry: { type: 'Polygon', coordinates: [coords] }
  };
}

export function collectGeometryCoords(geom) {
  const coords = [];
  if (!geom || !geom.coordinates) return coords;
  if (geom.type === 'Polygon') coords.push(...geom.coordinates[0]);
  if (geom.type === 'MultiPolygon') {
    for (const poly of geom.coordinates) coords.push(...poly[0]);
  }
  return coords;
}

export function computeGeometryCenter(geometry) {
  if (!geometry?.coordinates) return [0, 0];
  let sumLon = 0;
  let sumLat = 0;
  let count = 0;
  if (geometry.type === 'Polygon') {
    for (const pt of geometry.coordinates[0]) {
      sumLon += pt[0];
      sumLat += pt[1];
      count++;
    }
  } else if (geometry.type === 'MultiPolygon') {
    for (const poly of geometry.coordinates) {
      for (const pt of poly[0]) {
        sumLon += pt[0];
        sumLat += pt[1];
        count++;
      }
    }
  }
  return count > 0 ? [sumLon / count, sumLat / count] : [0, 0];
}

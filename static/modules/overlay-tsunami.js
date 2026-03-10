import EventAnimator, { AnimationMode } from './event-animator.js';

export function handleTsunamiRunups(controller, data, deps) {
  const { MapAdapter, TimeSlider, dataCache, yearRangeCache } = deps;
  const { geojson, eventId, runupCount } = data;
  console.log(`OverlayController: Starting tsunami runups animation for ${eventId} with ${runupCount} runups`);
  if (!geojson?.features || geojson.features.length < 2) return console.warn('OverlayController: Not enough data for tsunami animation');
  const sourceEvent = geojson.features.find(f => f.properties?.is_source === true);
  const sourceCoords = sourceEvent?.geometry?.coordinates;
  if (!sourceCoords) return console.warn('OverlayController: No source event found in tsunami data');
  MapAdapter?.hidePopup?.();
  MapAdapter.popupLocked = false;
  MapAdapter.map.flyTo({ center: sourceCoords, zoom: 7, duration: 1500 });
  const sourceYear = sourceEvent.properties?.year || new Date().getFullYear();
  const sourceDate = sourceEvent.properties?.timestamp ? new Date(sourceEvent.properties.timestamp).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }) : sourceYear;
  const started = EventAnimator.start({
    id: `tsunami-${eventId}`,
    label: `Tsunami ${sourceDate}`,
    mode: AnimationMode.RADIAL,
    events: geojson.features,
    eventType: 'tsunami',
    timeField: 'timestamp',
    granularity: '12m',
    renderer: 'point-radius',
    rendererOptions: { eventType: 'tsunami' },
    onExit: () => {
      const currentYear = controller.getCurrentYear();
      if (dataCache.tsunamis) controller.renderFilteredData('tsunamis', currentYear);
      controller.recalculateTimeRange();
      if (TimeSlider && Object.keys(yearRangeCache).length > 0) TimeSlider.show();
    }
  });
  if (!started) {
    const currentYear = controller.getCurrentYear();
    if (dataCache.tsunamis) controller.renderFilteredData('tsunamis', currentYear);
  }
}

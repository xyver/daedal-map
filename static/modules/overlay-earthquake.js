import EventAnimator, { AnimationMode } from './event-animator.js';

export async function handleSequenceChange(controller, sequenceId, eventId, deps) {
  const { ModelRegistry, OverlaySelector, OVERLAY_ENDPOINTS, TimeSlider, dataCache, yearRangeCache, gardnerKnopoffTimeWindow, fetchMsgpack } = deps;
  if (EventAnimator.getIsActive()) EventAnimator.stop();
  if (!sequenceId && !eventId) {
    const currentYear = controller.getCurrentYear();
    if (dataCache.earthquakes) controller.renderFilteredData('earthquakes', currentYear);
    return;
  }
  const cacheKey = eventId || sequenceId;
  let seqEvents;
  try {
    let endpoint;
    if (eventId) endpoint = `/api/earthquakes/aftershocks/${encodeURIComponent(eventId)}`;
    else endpoint = `/api/earthquakes/sequence/${encodeURIComponent(sequenceId)}`;
    const data = await fetchMsgpack(endpoint);
    seqEvents = data.features || [];
  } catch (error) {
    console.error('OverlayController: Error fetching sequence data:', error);
    return;
  }
  if (!seqEvents.length) return console.warn(`OverlayController: No events found for sequence ${cacheKey}`);
  let mainshock = seqEvents.find(f => f.properties.is_mainshock);
  if (!mainshock) mainshock = seqEvents.reduce((max, f) => ((f.properties.magnitude || 0) > (max.properties.magnitude || 0) ? f : max));
  const mainMag = mainshock.properties.magnitude || 5.5;
  const mainTime = new Date(mainshock.properties.timestamp || mainshock.properties.time).getTime();
  const windowEnd = mainTime + gardnerKnopoffTimeWindow(mainMag) * 24 * 60 * 60 * 1000;
  let minTime = mainTime;
  let maxTime = mainTime;
  for (const event of seqEvents) {
    const t = new Date(event.properties.timestamp || event.properties.time).getTime();
    if (t < minTime) minTime = t;
    if (t > maxTime) maxTime = t;
  }
  maxTime = Math.max(maxTime, windowEnd);
  const timeRange = maxTime - minTime;
  const stepHours = Math.max(1, Math.ceil((timeRange / (60 * 60 * 1000)) / 200));
  let granularityLabel = '6h';
  if (stepHours < 2) granularityLabel = '1h';
  else if (stepHours < 4) granularityLabel = '2h';
  else if (stepHours < 8) granularityLabel = '6h';
  else if (stepHours < 16) granularityLabel = '12h';
  else if (stepHours < 36) granularityLabel = 'daily';
  else granularityLabel = '2d';
  const label = `M${mainMag.toFixed(1)} ${new Date(mainTime).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })}`;
  const activeOverlays = OverlaySelector?.getActiveOverlays() || [];
  const overlaysToRestore = activeOverlays.filter(id => id !== 'demographics' && OVERLAY_ENDPOINTS[id]);
  const model = ModelRegistry?.getModelForType('earthquake');
  if (model?.clear) model.clear();
  EventAnimator.start({
    id: `seq-${sequenceId.substring(0, 8)}`,
    label,
    mode: AnimationMode.EARTHQUAKE,
    events: seqEvents,
    mainshock,
    eventType: 'earthquake',
    timeField: 'timestamp',
    granularity: granularityLabel,
    renderer: 'point-radius',
    onExit: () => {
      controller.recalculateTimeRange();
      if (TimeSlider) {
        if (TimeSlider.scales?.find(s => s.id === 'primary')) TimeSlider.setActiveScale('primary');
        if (Object.keys(yearRangeCache).length > 0) TimeSlider.show();
      }
      const currentYear = controller.getCurrentYear();
      for (const overlayId of overlaysToRestore) {
        if (dataCache[overlayId]) controller.renderFilteredData(overlayId, currentYear);
      }
    }
  });
}

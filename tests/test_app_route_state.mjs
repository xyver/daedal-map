import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  buildShareStateUrl,
  parseRouteIntent,
  writeEntityParam,
  writeLane
} from '../static/modules/routing/app-route-state.js';

function mockWindow(pathname, search = '') {
  const calls = [];
  globalThis.window = {
    location: { pathname, search, hash: '', origin: 'https://app.daedalmap.com' },
    history: {
      pushState: (_state, _title, url) => calls.push(['push', url]),
      replaceState: (_state, _title, url) => calls.push(['replace', url])
    }
  };
  return calls;
}

test('root parses Explore entry intent and Ops keeps its own lane', () => {
  const explore = parseRouteIntent({ pathname: '/', search: '?view=ring_of_fire&pack=earthquakes' });
  assert.equal(explore.lane, 'explore');
  assert.equal(explore.pack_id, 'earthquakes');
  assert.equal(explore.view_id, 'ring_of_fire');

  const ops = parseRouteIntent({ pathname: '/ops', search: '?feed=hurricanes_live&event_id=storm-1' });
  assert.equal(ops.lane, 'ops');
  assert.equal(ops.feed_id, 'hurricanes_live');
  assert.equal(ops.event_id, 'storm-1');
});

test('new Explore entity and share links use root while Ops links keep /ops', () => {
  const calls = mockWindow('/');
  writeEntityParam('explore', { packId: 'earthquakes', eventId: 'EQ-1' });
  assert.deepEqual(calls.pop(), ['replace', '/?pack=earthquakes&event_id=EQ-1']);

  writeEntityParam('ops', { feedId: 'hurricanes_live', eventId: 'storm-1' });
  assert.deepEqual(calls.pop(), ['replace', '/ops?feed=hurricanes_live&event_id=storm-1']);

  assert.match(buildShareStateUrl({ lane: 'explore', overlays: ['earthquakes'] }), /^\/\?/);
  assert.match(buildShareStateUrl({ lane: 'ops', overlays: ['earthquakes'] }), /^\/ops\?/);
});

test('switching from Ops to Explore writes the root URL', () => {
  const calls = mockWindow('/ops', '?feed=hurricanes_live');
  writeLane('explore');
  assert.deepEqual(calls, [['push', '/']]);
});

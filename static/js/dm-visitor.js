/* GENERATED FILE - DO NOT EDIT.
 * Copied from county-map-private/live/site/static/js/dm-visitor.js by
 * county-map-private/build/sync_shared_frontend_assets.py.
 * Edit the source there and re-run that script.
 */
/* DaedalMap cross-surface visitor identity and first-touch attribution.
 *
 * One visitor id and one acquisition record, shared by www.daedalmap.com,
 * app.daedalmap.com, and the /downloadable storefront, so a single person
 * moving between product surfaces is one row in analytics instead of several.
 *
 * This file is the canonical copy. The storefront uses it directly because
 * the main site serves /downloadable. Change this file rather than creating a
 * storefront copy.
 *
 * Identity boundary, which matters more than anything else here:
 *
 *   dm_anon  server-issued, HMAC-signed, HttpOnly. Owns quota, rate limiting,
 *            and settlement binding. See county-map/mapmover/caller_identity.py.
 *   dm_vid   client-issued, readable, analytics only. NEVER quota authority,
 *            NEVER spend authority, NEVER an input to pricing or permission.
 *
 * A caller can trivially forge dm_vid, which is fine for counting funnels and
 * unacceptable for anything that costs money. Nothing on the server may read
 * dm_vid to make an access or billing decision.
 *
 * Lifetime is 400 days: Chrome clamps cookie Max-Age to 400 days, and that
 * also sits at the 13-month ceiling CNIL and the ICO point to for analytics
 * identifiers. Longer values are silently truncated by the browser anyway.
 *
 * Consent. EU/EEA, UK, and Swiss visitors must opt in before analytics
 * cookies are set. The www server sets dm_region ("consent" or "notice") on
 * the parent domain from Cloudflare's country header; app.daedalmap.com is
 * DNS-only and the storefront is static, so they read that cookie. With no
 * region known, the visitor is treated as needing consent. Until analytics
 * are allowed, this file mints no dm_vid/dm_ft and pages must not load GA:
 * they put their GA bootstrap in dmVisitor.whenAnalyticsAllowed(fn).
 * dm_consent records the visitor's choice; dmVisitor.openSettings() reopens
 * the banner from a footer or privacy-page link.
 */
(function (global) {
  "use strict";

  var VISITOR_COOKIE = "dm_vid";
  var FIRST_TOUCH_COOKIE = "dm_ft";
  var OPT_OUT_COOKIE = "dm_no_ga";
  var OPT_OUT_STORAGE_KEY = "dm_no_ga";
  var MAX_AGE_SECONDS = 400 * 24 * 60 * 60;
  var CONSENT_COOKIE = "dm_consent";
  var REGION_COOKIE = "dm_region";
  // CNIL recommends asking again after about six months.
  var CONSENT_MAX_AGE_SECONDS = 182 * 24 * 60 * 60;
  var GA_MEASUREMENT_ID = "G-WTF95W3759";
  var PRIVACY_URL = "https://www.daedalmap.com/privacy";

  // Our own hosts. A referrer from one of these is internal navigation, not an
  // acquisition source, and must never overwrite a real first touch.
  var OWN_HOST_SUFFIXES = ["daedalmap.com", "daedalmap.io"];
  var LOCAL_HOSTS = ["localhost", "127.0.0.1", "0.0.0.0", "::1"];

  function hostname() {
    return String(global.location && global.location.hostname || "").toLowerCase();
  }

  function isLocalHost() {
    return LOCAL_HOSTS.indexOf(hostname()) !== -1;
  }

  function isOwnHost(host) {
    var value = String(host || "").toLowerCase();
    for (var i = 0; i < OWN_HOST_SUFFIXES.length; i += 1) {
      var suffix = OWN_HOST_SUFFIXES[i];
      if (value === suffix || value.slice(-(suffix.length + 1)) === "." + suffix) return true;
    }
    return false;
  }

  /* Cookie domain wide enough to span www/app/downloads. Returns "" on
   * localhost, where a domain attribute would make the cookie unsettable. */
  function cookieDomain() {
    var host = hostname();
    for (var i = 0; i < OWN_HOST_SUFFIXES.length; i += 1) {
      var suffix = OWN_HOST_SUFFIXES[i];
      if (host === suffix || host.slice(-(suffix.length + 1)) === "." + suffix) return "." + suffix;
    }
    return "";
  }

  function readCookie(name) {
    try {
      var parts = String(global.document.cookie || "").split(";");
      for (var i = 0; i < parts.length; i += 1) {
        var part = parts[i].trim();
        if (part.indexOf(name + "=") === 0) {
          return decodeURIComponent(part.slice(name.length + 1));
        }
      }
    } catch (e) {}
    return "";
  }

  function writeCookie(name, value, maxAgeSeconds) {
    try {
      var domain = cookieDomain();
      var parts = [
        name + "=" + encodeURIComponent(value),
        "path=/",
        "max-age=" + Math.floor(maxAgeSeconds),
        "SameSite=Lax"
      ];
      if (domain) parts.push("domain=" + domain);
      if (global.location.protocol === "https:") parts.push("Secure");
      global.document.cookie = parts.join("; ");
    } catch (e) {}
  }

  function deleteCookie(name) {
    writeCookie(name, "", 0);
  }

  /* The owner kill-switch. Historically a localStorage flag, which is
   * per-origin and therefore did not follow the owner from www to downloads.
   * It is now primarily a cookie on the shared parent domain; the old
   * localStorage flag is still honored so browsers already opted out stay
   * opted out without revisiting ?noga=1. */
  function suppressed() {
    if (isLocalHost()) return true;
    try {
      var params = new global.URLSearchParams(global.location.search);
      if (params.get("noga") === "1") {
        writeCookie(OPT_OUT_COOKIE, "1", MAX_AGE_SECONDS);
        try { global.localStorage.setItem(OPT_OUT_STORAGE_KEY, "1"); } catch (e) {}
        return true;
      }
      if (params.get("noga") === "0") {
        deleteCookie(OPT_OUT_COOKIE);
        try { global.localStorage.removeItem(OPT_OUT_STORAGE_KEY); } catch (e) {}
        return false;
      }
    } catch (e) {}
    if (readCookie(OPT_OUT_COOKIE) === "1") return true;
    try {
      if (global.localStorage.getItem(OPT_OUT_STORAGE_KEY) === "1") {
        // Migrate the per-origin flag onto the shared cookie so the opt-out
        // follows this browser across all three hosts from now on.
        writeCookie(OPT_OUT_COOKIE, "1", MAX_AGE_SECONDS);
        return true;
      }
    } catch (e) {}
    return false;
  }

  function randomId() {
    var bytes = new Uint8Array(16);
    try {
      global.crypto.getRandomValues(bytes);
    } catch (e) {
      for (var i = 0; i < bytes.length; i += 1) bytes[i] = Math.floor(Math.random() * 256);
    }
    var out = "";
    for (var j = 0; j < bytes.length; j += 1) {
      out += ("0" + bytes[j].toString(16)).slice(-2);
    }
    return "v1." + out;
  }

  /* Attribution values become GA dimension values and Supabase column values,
   * so they are aggressively bounded. Anything unexpected collapses to a short
   * safe token rather than becoming a new high-cardinality dimension value. */
  function slug(value, maxLength) {
    return String(value || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9._\-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, maxLength || 60);
  }

  function safePath(value, maxLength) {
    return String(value || "")
      .trim()
      .replace(/[^A-Za-z0-9/._\-]+/g, "")
      .slice(0, maxLength || 120);
  }

  function referrerHost() {
    try {
      if (!global.document.referrer) return "";
      return new global.URL(global.document.referrer).hostname.replace(/^www\./, "");
    } catch (e) {
      return "";
    }
  }

  /* First touch is written exactly once per browser and never overwritten.
   * Last-touch is GA's job through sessionSourceMedium; duplicating it here
   * would just be a worse copy of a report GA already renders correctly. */
  function computeFirstTouch() {
    var params;
    try {
      params = new global.URLSearchParams(global.location.search);
    } catch (e) {
      params = { get: function () { return null; } };
    }
    var refHost = referrerHost();
    var internal = isOwnHost(refHost);
    var utmSource = slug(params.get("utm_source"), 60);
    var utmMedium = slug(params.get("utm_medium"), 40);

    var source = utmSource;
    if (!source) {
      if (refHost && !internal) source = slug(refHost, 60);
      else source = "direct";
    }
    var medium = utmMedium;
    if (!medium) {
      if (utmSource) medium = "campaign";
      else if (refHost && !internal) medium = "referral";
      else medium = "none";
    }
    return {
      src: source,
      med: medium,
      cmp: slug(params.get("utm_campaign"), 60) || "none",
      lp: safePath(global.location.pathname, 120) || "/",
      host: slug(hostname(), 40),
      ts: new Date().toISOString().slice(0, 10)
    };
  }

  function encodeFirstTouch(record) {
    var pairs = [];
    for (var key in record) {
      if (Object.prototype.hasOwnProperty.call(record, key) && record[key]) {
        pairs.push(key + "=" + encodeURIComponent(record[key]));
      }
    }
    return pairs.join("&");
  }

  function decodeFirstTouch(value) {
    if (!value) return null;
    var record = {};
    var pairs = String(value).split("&");
    for (var i = 0; i < pairs.length; i += 1) {
      var index = pairs[i].indexOf("=");
      if (index <= 0) continue;
      var key = pairs[i].slice(0, index);
      try {
        record[key] = decodeURIComponent(pairs[i].slice(index + 1)).slice(0, 120);
      } catch (e) {
        record[key] = "";
      }
    }
    return record.src ? record : null;
  }

  /* Remove Google linker transport parameters after attribution has had a
   * chance to inspect the landing URL. DaedalMap surfaces share one parent
   * domain and do not need cross-domain decoration; old decorated links should
   * still settle to a clean canonical URL in the address bar. */
  function stripGoogleLinkerParams() {
    try {
      if (!global.history || !global.history.replaceState) return;
      var url = new global.URL(global.location.href);
      var changed = false;
      var keys = [];
      url.searchParams.forEach(function (_value, key) { keys.push(key); });
      for (var i = 0; i < keys.length; i += 1) {
        if (keys[i] === "_gl" || keys[i] === "_ga" || keys[i].indexOf("_ga_") === 0) {
          url.searchParams.delete(keys[i]);
          changed = true;
        }
      }
      if (changed) {
        global.history.replaceState(global.history.state, "", url.pathname + url.search + url.hash);
      }
    } catch (e) {}
  }

  var state = {
    suppressed: true,
    consent: "pending",
    visitorId: "",
    firstTouch: null
  };
  var allowedCallbacks = [];

  /* "granted", "denied", or "pending" (banner needed). Outside consent
   * regions analytics default on, and an explicit "denied" still wins. */
  function consentState() {
    var choice = readCookie(CONSENT_COOKIE);
    if (choice === "granted" || choice === "denied") return choice;
    return readCookie(REGION_COOKIE) === "notice" ? "granted" : "pending";
  }

  function analyticsAllowed() {
    return !state.suppressed && state.consent === "granted";
  }

  /* Remove every analytics cookie this browser holds for our domains. GA
   * cookies are _ga and _ga_<container>; they may sit on the parent domain or
   * the exact host, so both are cleared. */
  function clearAnalyticsCookies() {
    var names = [VISITOR_COOKIE, FIRST_TOUCH_COOKIE];
    try {
      var parts = String(global.document.cookie || "").split(";");
      for (var i = 0; i < parts.length; i += 1) {
        var name = parts[i].split("=")[0].trim();
        if (name === "_ga" || name.indexOf("_ga_") === 0) names.push(name);
      }
    } catch (e) {}
    for (var j = 0; j < names.length; j += 1) {
      deleteCookie(names[j]);
      try { global.document.cookie = names[j] + "=; path=/; max-age=0"; } catch (e) {}
    }
    state.visitorId = "";
    state.firstTouch = null;
  }

  function mintIdentity() {
    state.visitorId = readCookie(VISITOR_COOKIE);
    if (!state.visitorId || state.visitorId.indexOf("v1.") !== 0) {
      state.visitorId = randomId();
    }
    // Refresh on every visit so an active visitor keeps a rolling 400-day
    // window rather than expiring 400 days after their very first visit.
    writeCookie(VISITOR_COOKIE, state.visitorId, MAX_AGE_SECONDS);

    state.firstTouch = decodeFirstTouch(readCookie(FIRST_TOUCH_COOKIE));
    if (!state.firstTouch) {
      state.firstTouch = computeFirstTouch();
      writeCookie(FIRST_TOUCH_COOKIE, encodeFirstTouch(state.firstTouch), MAX_AGE_SECONDS);
    }
  }

  function runAllowedCallbacks() {
    var pending = allowedCallbacks;
    allowedCallbacks = [];
    for (var i = 0; i < pending.length; i += 1) {
      try { pending[i](); } catch (e) {}
    }
  }

  function setConsent(choice) {
    writeCookie(CONSENT_COOKIE, choice, CONSENT_MAX_AGE_SECONDS);
    state.consent = choice;
    if (choice === "granted") {
      if (state.suppressed) return;
      mintIdentity();
      if (typeof global.gtag === "function") {
        try { global.gtag("consent", "update", { analytics_storage: "granted" }); } catch (e) {}
      }
      global["ga-disable-" + GA_MEASUREMENT_ID] = false;
      runAllowedCallbacks();
      return;
    }
    // A loaded gtag cannot be unloaded; disabling the property stops further
    // hits on this page, and the cookies go now.
    global["ga-disable-" + GA_MEASUREMENT_ID] = true;
    if (typeof global.gtag === "function") {
      try { global.gtag("consent", "update", { analytics_storage: "denied" }); } catch (e) {}
    }
    clearAnalyticsCookies();
  }

  /* Consent banner. Plain DOM with inline styles so www, app, and the static
   * storefront render it identically without a shared stylesheet. Accept and
   * Decline are equal in size and weight, as CNIL and the ICO expect. */
  var BANNER_ID = "dm-consent-banner";

  function closeBanner() {
    var existing = global.document.getElementById(BANNER_ID);
    if (existing && existing.parentNode) existing.parentNode.removeChild(existing);
  }

  function buttonCss() {
    return "flex:1 1 0;min-width:110px;padding:10px 16px;border-radius:8px;" +
      "border:1px solid rgba(126,206,255,.45);background:#0d2236;color:#e7f2fb;" +
      "font:600 14px system-ui,-apple-system,Segoe UI,sans-serif;cursor:pointer";
  }

  function showBanner() {
    var doc = global.document;
    if (!doc || !doc.body || doc.getElementById(BANNER_ID)) return;
    var banner = doc.createElement("div");
    banner.id = BANNER_ID;
    banner.setAttribute("role", "dialog");
    banner.setAttribute("aria-label", "Analytics cookies");
    banner.style.cssText = "position:fixed;left:16px;right:16px;bottom:16px;z-index:2147483000;" +
      "max-width:640px;margin:0 auto;padding:16px 18px;border-radius:12px;" +
      "background:#081a2a;color:#e7f2fb;border:1px solid rgba(116,185,224,.3);" +
      "box-shadow:0 12px 40px rgba(0,0,0,.45);" +
      "font:14px/1.5 system-ui,-apple-system,Segoe UI,sans-serif";

    var text = doc.createElement("p");
    text.style.cssText = "margin:0 0 12px";
    text.appendChild(doc.createTextNode(
      "DaedalMap would like to use analytics cookies (Google Analytics and a " +
      "DaedalMap visitor id) to see which pages and tools are used. They stay " +
      "off unless you accept. "
    ));
    var link = doc.createElement("a");
    link.href = PRIVACY_URL;
    link.textContent = "Privacy policy";
    link.style.cssText = "color:#7eceff";
    text.appendChild(link);
    banner.appendChild(text);

    var row = doc.createElement("div");
    row.style.cssText = "display:flex;gap:10px;flex-wrap:wrap";
    var choices = [["Accept", "granted"], ["Decline", "denied"]];
    for (var i = 0; i < choices.length; i += 1) {
      (function (label, choice) {
        var button = doc.createElement("button");
        button.type = "button";
        button.textContent = label;
        button.setAttribute("data-consent", choice);
        button.style.cssText = buttonCss();
        button.addEventListener("click", function () {
          setConsent(choice);
          closeBanner();
        });
        row.appendChild(button);
      })(choices[i][0], choices[i][1]);
    }
    banner.appendChild(row);
    doc.body.appendChild(banner);
  }

  function whenBodyReady(fn) {
    var doc = global.document;
    if (doc.body) { fn(); return; }
    doc.addEventListener("DOMContentLoaded", fn);
  }

  function init() {
    state.suppressed = suppressed();
    // Deliberately do not mint an id for a suppressed browser. An owner
    // browsing with ?noga=1 should leave no analytics identity behind at
    // all, not merely be filtered out downstream. The same holds until a
    // consent-region visitor accepts.
    state.consent = consentState();
    if (analyticsAllowed()) mintIdentity();
    stripGoogleLinkerParams();
    if (!state.suppressed && state.consent === "pending") whenBodyReady(showBanner);
  }

  init();

  var api = {
    /* Random per-browser analytics id, or "" when suppressed. */
    id: function () { return state.visitorId; },

    /* First-touch acquisition record, or null when suppressed. */
    firstTouch: function () { return state.firstTouch ? Object.assign({}, state.firstTouch) : null; },

    suppressed: function () { return state.suppressed; },

    /* True when analytics may run: not suppressed and consent granted (or
     * granted by default outside consent regions). */
    analyticsAllowed: function () { return analyticsAllowed(); },

    /* "granted", "denied", or "pending". */
    consent: function () { return state.consent; },

    /* Run fn now if analytics are allowed, otherwise once the visitor
     * accepts. Pages put their GA bootstrap here and nowhere else. */
    whenAnalyticsAllowed: function (fn) {
      if (typeof fn !== "function") return;
      if (analyticsAllowed()) {
        try { fn(); } catch (e) {}
        return;
      }
      // Queued even after a decline, so accepting later from openSettings()
      // starts analytics on the current page.
      if (!state.suppressed) allowedCallbacks.push(fn);
    },

    /* Reopen the banner so a visitor can change an earlier choice. */
    openSettings: function () {
      if (state.suppressed) return;
      closeBanner();
      whenBodyReady(showBanner);
    },

    /* Bounded, low-cardinality params for GA events. The visitor id is
     * deliberately absent: GA is not the join surface, Supabase is, and
     * registering a per-browser id as a GA dimension would blow up cardinality
     * for no reporting benefit. */
    gaParams: function () {
      var touch = state.firstTouch;
      if (!touch) return {};
      return {
        dm_src: touch.src || "direct",
        dm_med: touch.med || "none",
        dm_cmp: touch.cmp || "none",
        dm_lp: touch.lp || "/"
      };
    },

    /* Allowlisted context for server-side analytics rows. Every value is a
     * short bounded string; callers must not add free-text or payload fields. */
    context: function () {
      var touch = state.firstTouch || {};
      var out = {};
      if (state.visitorId) out.visitor_id = state.visitorId;
      if (touch.src) out.first_touch_source = touch.src;
      if (touch.med) out.first_touch_medium = touch.med;
      if (touch.cmp) out.first_touch_campaign = touch.cmp;
      if (touch.lp) out.first_touch_landing = touch.lp;
      if (touch.ts) out.first_touch_date = touch.ts;
      return out;
    },

    /* Append visitor context to an outbound URL, used for the download
     * redirect so a click on the R2-hosted storefront still resolves to a
     * visitor on the site server. */
    decorate: function (url, extra) {
      try {
        var parsed = new global.URL(url, global.location.href);
        var context = api.context();
        for (var key in context) {
          if (Object.prototype.hasOwnProperty.call(context, key)) {
            parsed.searchParams.set(key, context[key]);
          }
        }
        if (extra) {
          for (var extraKey in extra) {
            if (Object.prototype.hasOwnProperty.call(extra, extraKey) && extra[extraKey]) {
              parsed.searchParams.set(extraKey, String(extra[extraKey]).slice(0, 120));
            }
          }
        }
        return parsed.href;
      } catch (e) {
        return url;
      }
    }
  };

  global.dmVisitor = api;
})(window);

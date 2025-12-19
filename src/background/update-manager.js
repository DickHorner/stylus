import compareVersion from '@/js/cmpver';
import {UCD} from '@/js/consts';
import * as prefs from '@/js/prefs';
import {calcStyleDigest, styleSectionsEqual} from '@/js/sections-util';
import {chromeLocal} from '@/js/storage-util';
import {extractUsoaId, isCdnUrl, isLocalhost, rxGF, usoApi} from '@/js/urls';
import {debounce, deepMerge, getHost, NOP, sleep} from '@/js/util';
import {ignoreChromeError} from '@/js/util-webext';
import {bgBusy} from './common';
import {db} from './db';
import download from './download';
import * as styleMan from './style-manager';
import * as usercssMan from './usercss-manager';
import {getEmbeddedMeta, toUsercss} from './uso-api';

const STATES = /** @namespace UpdaterStates */ {
  UPDATED: 'updated',
  SKIPPED: 'skipped',
  UNREACHABLE: 'server unreachable',
  // details for SKIPPED status
  EDITED:        'locally edited',
  MAYBE_EDITED:  'may be locally edited',
  SAME_MD5:      'up-to-date: MD5 is unchanged',
  SAME_CODE:     'up-to-date: code sections are unchanged',
  SAME_VERSION:  'up-to-date: version is unchanged',
  ERROR_MD5:     'error: MD5 is invalid',
  ERROR_JSON:    'error: JSON is invalid',
  ERROR_VERSION: 'error: version is older than installed style',
};
export const getStates = () => STATES;
const safeSleep = __.MV3 ? ms => __.KEEP_ALIVE(sleep(ms)) : sleep;
const RH_ETAG = {responseHeaders: ['etag']}; // a hashsum of file contents
const RX_DATE2VER = new RegExp([
  /^(\d{4})/,
  /(0[1-9]|1(?:0|[12](?=\d\d))?|[2-9])/, // in ambiguous cases like yyyy123 the month will be 1
  /(0[1-9]|[1-2][0-9]?|3[0-1]?|[4-9])/,
  /\.([01][0-9]?|2[0-3]?|[3-9])/,
  /\.([0-5][0-9]?|[6-9])$/,
].map(rx => rx.source).join(''));
const ALARM_NAME = 'scheduledUpdate';
const MIN_INTERVAL_MS = 60e3;
const RETRY_ERRORS = [
  503, // service unavailable
  429, // too many requests
];
const HOST_THROTTLE = 1000; // ms
const hostJobs = {};
let lastUpdateTime;
let checkingAll = false;
let logQueue = [];
let logLastWriteTime = 0;

// Compute MD5 hash for integrity verification
// NOTE: MD5 is cryptographically weak and vulnerable to collision attacks.
// This implementation is used ONLY because the server provides MD5 hashes.
// It protects against accidental corruption and casual tampering, but NOT
// against sophisticated attacks. A migration to SHA-256 would require
// server-side changes to provide SHA-256 hashes instead of MD5.
function computeMd5(str) {
  // Simple MD5 hash implementation
  // This is a basic implementation for integrity checking
  function md5cycle(x, k) {
    let a = x[0]; let b = x[1]; let c = x[2]; let
      d = x[3];
    a = ff(a, b, c, d, k[0], 7, -680876936);
    d = ff(d, a, b, c, k[1], 12, -389564586);
    c = ff(c, d, a, b, k[2], 17, 606105819);
    b = ff(b, c, d, a, k[3], 22, -1044525330);
    a = ff(a, b, c, d, k[4], 7, -176418897);
    d = ff(d, a, b, c, k[5], 12, 1200080426);
    c = ff(c, d, a, b, k[6], 17, -1473231341);
    b = ff(b, c, d, a, k[7], 22, -45705983);
    a = ff(a, b, c, d, k[8], 7, 1770035416);
    d = ff(d, a, b, c, k[9], 12, -1958414417);
    c = ff(c, d, a, b, k[10], 17, -42063);
    b = ff(b, c, d, a, k[11], 22, -1990404162);
    a = ff(a, b, c, d, k[12], 7, 1804603682);
    d = ff(d, a, b, c, k[13], 12, -40341101);
    c = ff(c, d, a, b, k[14], 17, -1502002290);
    b = ff(b, c, d, a, k[15], 22, 1236535329);
    a = gg(a, b, c, d, k[1], 5, -165796510);
    d = gg(d, a, b, c, k[6], 9, -1069501632);
    c = gg(c, d, a, b, k[11], 14, 643717713);
    b = gg(b, c, d, a, k[0], 20, -373897302);
    a = gg(a, b, c, d, k[5], 5, -701558691);
    d = gg(d, a, b, c, k[10], 9, 38016083);
    c = gg(c, d, a, b, k[15], 14, -660478335);
    b = gg(b, c, d, a, k[4], 20, -405537848);
    a = gg(a, b, c, d, k[9], 5, 568446438);
    d = gg(d, a, b, c, k[14], 9, -1019803690);
    c = gg(c, d, a, b, k[3], 14, -187363961);
    b = gg(b, c, d, a, k[8], 20, 1163531501);
    a = gg(a, b, c, d, k[13], 5, -1444681467);
    d = gg(d, a, b, c, k[2], 9, -51403784);
    c = gg(c, d, a, b, k[7], 14, 1735328473);
    b = gg(b, c, d, a, k[12], 20, -1926607734);
    a = hh(a, b, c, d, k[5], 4, -378558);
    d = hh(d, a, b, c, k[8], 11, -2022574463);
    c = hh(c, d, a, b, k[11], 16, 1839030562);
    b = hh(b, c, d, a, k[14], 23, -35309556);
    a = hh(a, b, c, d, k[1], 4, -1530992060);
    d = hh(d, a, b, c, k[4], 11, 1272893353);
    c = hh(c, d, a, b, k[7], 16, -155497632);
    b = hh(b, c, d, a, k[10], 23, -1094730640);
    a = hh(a, b, c, d, k[13], 4, 681279174);
    d = hh(d, a, b, c, k[0], 11, -358537222);
    c = hh(c, d, a, b, k[3], 16, -722521979);
    b = hh(b, c, d, a, k[6], 23, 76029189);
    a = hh(a, b, c, d, k[9], 4, -640364487);
    d = hh(d, a, b, c, k[12], 11, -421815835);
    c = hh(c, d, a, b, k[15], 16, 530742520);
    b = hh(b, c, d, a, k[2], 23, -995338651);
    a = ii(a, b, c, d, k[0], 6, -198630844);
    d = ii(d, a, b, c, k[7], 10, 1126891415);
    c = ii(c, d, a, b, k[14], 15, -1416354905);
    b = ii(b, c, d, a, k[5], 21, -57434055);
    a = ii(a, b, c, d, k[12], 6, 1700485571);
    d = ii(d, a, b, c, k[3], 10, -1894986606);
    c = ii(c, d, a, b, k[10], 15, -1051523);
    b = ii(b, c, d, a, k[1], 21, -2054922799);
    a = ii(a, b, c, d, k[8], 6, 1873313359);
    d = ii(d, a, b, c, k[15], 10, -30611744);
    c = ii(c, d, a, b, k[6], 15, -1560198380);
    b = ii(b, c, d, a, k[13], 21, 1309151649);
    a = ii(a, b, c, d, k[4], 6, -145523070);
    d = ii(d, a, b, c, k[11], 10, -1120210379);
    c = ii(c, d, a, b, k[2], 15, 718787259);
    b = ii(b, c, d, a, k[9], 21, -343485551);
    x[0] = add32(a, x[0]);
    x[1] = add32(b, x[1]);
    x[2] = add32(c, x[2]);
    x[3] = add32(d, x[3]);
  }

  function cmn(q, a, b, x, s, t) {
    a = add32(add32(a, q), add32(x, t));
    return add32((a << s) | (a >>> (32 - s)), b);
  }

  function ff(a, b, c, d, x, s, t) {
    return cmn((b & c) | ((~b) & d), a, b, x, s, t);
  }

  function gg(a, b, c, d, x, s, t) {
    return cmn((b & d) | (c & (~d)), a, b, x, s, t);
  }

  function hh(a, b, c, d, x, s, t) {
    return cmn(b ^ c ^ d, a, b, x, s, t);
  }

  function ii(a, b, c, d, x, s, t) {
    return cmn(c ^ (b | (~d)), a, b, x, s, t);
  }

  function add32(a, b) {
    return (a + b) & 0xFFFFFFFF;
  }

  function md51(s) {
    const n = s.length;
    const state = [1732584193, -271733879, -1732584194, 271733878];
    let i;
    for (i = 64; i <= s.length; i += 64) {
      md5cycle(state, md5blk(s.substring(i - 64, i)));
    }
    s = s.substring(i - 64);
    const tail = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    for (i = 0; i < s.length; i++)
      tail[i >> 2] |= s.charCodeAt(i) << ((i % 4) << 3);
    tail[i >> 2] |= 0x80 << ((i % 4) << 3);
    if (i > 55) {
      md5cycle(state, tail);
      for (i = 0; i < 16; i++) tail[i] = 0;
    }
    tail[14] = n * 8;
    md5cycle(state, tail);
    return state;
  }

  function md5blk(s) {
    const md5blks = [];
    for (let i = 0; i < 64; i += 4) {
      md5blks[i >> 2] = s.charCodeAt(i) +
        (s.charCodeAt(i + 1) << 8) +
        (s.charCodeAt(i + 2) << 16) +
        (s.charCodeAt(i + 3) << 24);
    }
    return md5blks;
  }

  const hexChr = '0123456789abcdef'.split('');

  function rhex(n) {
    let s = '';
    for (let j = 0; j < 4; j++)
      s += hexChr[(n >> (j * 8 + 4)) & 0x0F] + hexChr[(n >> (j * 8)) & 0x0F];
    return s;
  }

  function hex(x) {
    for (let i = 0; i < x.length; i++)
      x[i] = rhex(x[i]);
    return x.join('');
  }

  return hex(md51(str));
}

bgBusy.then(async () => {
  lastUpdateTime = await chromeLocal.getValue('lastUpdateTime');
  if (!lastUpdateTime) rememberNow();
  prefs.subscribe('updateInterval', schedule, true);
  chrome.alarms.onAlarm.addListener(onAlarm);
});

export async function checkAllStyles({
  save = true,
  ignoreDigest,
  observe,
  onlyEnabled = prefs.__values.updateOnlyEnabled,
} = {}) {
  rememberNow();
  schedule();
  checkingAll = true;
  const port = observe && chrome.runtime.connect({name: 'updater'});
  const styles = styleMan.getAll().filter(s =>
    s.updateUrl &&
    s.updatable !== false &&
    (!onlyEnabled || s.enabled));
  if (port) port.postMessage({count: styles.length});
  log('');
  log(`${save ? 'Scheduled' : 'Manual'} update check for ${styles.length} styles`);
  await Promise.all(
    styles.map(style =>
      checkStyle({style, port, save, ignoreDigest})));
  if (port) port.postMessage({done: true});
  if (port) port.disconnect();
  log('');
  checkingAll = false;
}

/**
 * @param {{
    id?: number,
    style?: StyleObj,
    port?: chrome.runtime.Port,
    save?: boolean,
    ignoreDigest?: boolean,
  }} opts
 * @returns {{
    style: StyleObj,
    updated?: boolean,
    error?: any,
    STATES: UpdaterStates,
   }}

 Original style digests are calculated in these cases:
 * style is installed or updated from server
 * non-usercss style is checked for an update and styleSectionsEqual considers it unchanged

 Update check proceeds in these cases:
 * style has the original digest and it's equal to the current digest
 * [ignoreDigest: true] style doesn't yet have the original digest but we ignore it
 * [ignoreDigest: none/false] style doesn't yet have the original digest
 so we compare the code to the server code and if it's the same we save the digest,
 otherwise we skip the style and report MAYBE_EDITED status

 'ignoreDigest' option is set on the second manual individual update check on the manage page.
 */
export async function checkStyle(opts) {
  let {id} = opts;
  const {
    style = styleMan.get(id),
    ignoreDigest,
    port,
    save,
  } = opts;
  if (!id) id = style.id;
  const {md5Url} = style;
  let {[UCD]: ucd, updateUrl} = style;
  let res, state;
  try {
    await checkIfEdited();
    res = {
      style: await (ucd && !md5Url ? updateUsercss : updateUSO)().then(maybeSave),
      updated: true,
    };
    state = STATES.UPDATED;
  } catch (err) {
    const error = err === 0 && STATES.UNREACHABLE ||
      err && err.message ||
      err;
    res = {error, style, STATES};
    state = `${STATES.SKIPPED} (${Array.isArray(err) ? err[0].message : error})`;
  }
  log(`${state} #${id} ${style.customName || style.name}`);
  if (port) port.postMessage(res);
  return res;

  async function checkIfEdited() {
    if (!ignoreDigest &&
        style.originalDigest &&
        style.originalDigest !== await calcStyleDigest(style)) {
      return Promise.reject(STATES.EDITED);
    }
  }

  async function updateUSO() {
    const md5 = await tryDownload(md5Url);
    if (!md5 || md5.length !== 32) {
      return Promise.reject(STATES.ERROR_MD5);
    }
    if (md5 === style.originalMd5 && style.originalDigest && !ignoreDigest) {
      return Promise.reject(STATES.SAME_MD5);
    }
    const usoId = +md5Url.match(/\/(\d+)/)[1];
    let varsUrl = '';
    if (!ucd) {
      ucd = {};
      varsUrl = updateUrl;
    }
    updateUrl = style.updateUrl = `${usoApi}Css/${usoId}`;
    const {result: css} = await tryDownload(updateUrl, {responseType: 'json'});

    // Verify MD5 integrity to detect corruption and casual tampering
    // NOTE: This provides protection against accidental corruption, not sophisticated attacks
    // JSON.stringify is used for non-string data; ensure this matches server's MD5 calculation
    const cssText = typeof css === 'string' ? css : JSON.stringify(css);
    const computedMd5 = computeMd5(cssText);
    if (computedMd5 !== md5) {
      console.error('MD5 integrity check failed for style update');
      return Promise.reject(STATES.ERROR_MD5);
    }

    const json = await updateUsercss(css)
      || await toUsercss(usoId, varsUrl, css, style, md5, md5Url);
    json.originalMd5 = md5;
    return json;
  }

  async function updateUsercss(css) {
    let oldVer = ucd.version;
    let oldEtag = style.etag;
    let m = (css || extractUsoaId(updateUrl)) &&
      await getEmbeddedMeta(css || style.sourceCode);
    if (m && m.updateUrl) {
      updateUrl = m.updateUrl;
      oldVer = m[UCD].version || '0';
      oldEtag = '';
    } else if (css) {
      return;
    }
    /* Using the more efficient HEAD+GET approach for greasyfork instead of GET+GET,
       because if ETAG header changes it normally means an update so we don't need to
       download meta additionally in a separate request. */
    if ((m = updateUrl.match(rxGF))[5] === 'meta')
      updateUrl = m[1] + 'user' + m[6];
    if (oldEtag && oldEtag === await downloadEtag(updateUrl)) {
      return Promise.reject(STATES.SAME_CODE);
    }
    // TODO: when sourceCode is > 100kB use http range request(s) for version check
    const {headers: {etag}, response} = await tryDownload(updateUrl, RH_ETAG);
    const json = await usercssMan.buildMeta({sourceCode: response, etag, updateUrl});
    const delta = compareVersion(json[UCD].version, oldVer);
    let err;
    if (!delta && !ignoreDigest) {
      // re-install is invalid in a soft upgrade
      err = response === style.sourceCode
        ? STATES.SAME_CODE
        : !isLocalhost(updateUrl) && STATES.SAME_VERSION;
    }
    if (delta < 0) {
      // downgrade is always invalid
      err = STATES.ERROR_VERSION;
    }
    if (err && etag && !style.etag) {
      // first check of ETAG, gonna write it directly to DB as it's too trivial to sync or announce
      style.etag = etag;
      await db.put(style);
    }
    return err
      ? Promise.reject(err)
      : json;
  }

  async function maybeSave(json) {
    json.id = id;
    // keep current state
    delete json.customName;
    delete json.enabled;
    const newStyle = Object.assign({}, style, json);
    newStyle.updateDate = getDateFromVer(newStyle) || Date.now();
    // update digest even if save === false as there might be just a space added etc.
    if (!ucd && styleSectionsEqual(json, style)) {
      style.originalDigest = (await styleMan.install(newStyle)).originalDigest;
      return Promise.reject(STATES.SAME_CODE);
    }
    if (!style.originalDigest && !ignoreDigest) {
      return Promise.reject(STATES.MAYBE_EDITED);
    }
    return !save ? newStyle :
      ucd ? usercssMan.install(newStyle, {dup: style})
        : styleMan.install(newStyle);
  }

}

async function tryDownload(url, params, {retryDelay = HOST_THROTTLE} = {}) {
  while (true) {
    let host, job;
    try {
      params = deepMerge(params || {}, {headers: {'Cache-Control': 'no-cache'}});
      host = getHost(url);
      job = hostJobs[host];
      job = hostJobs[host] = (job
        ? job.catch(NOP).then(() => safeSleep(HOST_THROTTLE / (isCdnUrl(url) ? 4 : 1)))
        : Promise.resolve()
      ).then(() => download(url, params));
      return await job;
    } catch (code) {
      if (!RETRY_ERRORS.includes(code) ||
          retryDelay > MIN_INTERVAL_MS) {
        throw code;
      }
    } finally {
      if (hostJobs[host] === job) delete hostJobs[host];
    }
    retryDelay *= 1.25;
    await safeSleep(retryDelay);
  }
}

async function downloadEtag(url) {
  const req = await tryDownload(url, {method: 'HEAD', ...RH_ETAG});
  return req.headers.etag;
}

function getDateFromVer(style) {
  const m = RX_DATE2VER.exec(style[UCD]?.version);
  if (m) {
    m[2]--; // month is 0-based in `Date` constructor
    return new Date(...m.slice(1)).getTime();
  }
}

function schedule() {
  const interval = prefs.__values.updateInterval * 60 * 60 * 1000;
  if (interval > 0) {
    const elapsed = Math.max(0, Date.now() - lastUpdateTime);
    chrome.alarms.create(ALARM_NAME, {
      when: Date.now() + Math.max(MIN_INTERVAL_MS, interval - elapsed),
    });
  } else {
    chrome.alarms.clear(ALARM_NAME, ignoreChromeError);
  }
}

async function onAlarm({name}) {
  if (name === ALARM_NAME) {
    if (bgBusy) await bgBusy;
    __.KEEP_ALIVE(checkAllStyles());
  }
}

function rememberNow() {
  chromeLocal.set({lastUpdateTime: lastUpdateTime = Date.now()});
}

function log(text) {
  logQueue.push({text, time: new Date().toLocaleString()});
  debounce(flushQueue, text && checkingAll ? 1000 : 0);
}

async function flushQueue(lines) {
  if (!lines) {
    flushQueue(await chromeLocal.getValue('updateLog') || []);
    return;
  }
  const time = Date.now() - logLastWriteTime > 11e3 ?
    logQueue[0].time + ' ' :
    '';
  if (logQueue[0] && !logQueue[0].text) {
    logQueue.shift();
    if (lines[lines.length - 1]) lines.push('');
  }
  lines.splice(0, lines.length - 1000);
  lines.push(time + (logQueue[0] && logQueue[0].text || ''));
  lines.push(...logQueue.slice(1).map(item => item.text));

  chromeLocal.set({updateLog: lines});
  logLastWriteTime = Date.now();
  logQueue = [];
}

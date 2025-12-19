import {k_busy, kInvokeAPI} from '@/js/consts';
import {bgReadySignal} from './msg-api';

/** @type {Map<function,boolean>} true: returned value is used as the reply */
export const onMessage = new Map();
export const onConnect = {};
export const onDisconnect = {};
export const wrapData = data => ({
  data,
});
export const wrapError = error => ({
  error: Object.assign({
    message: error.message || `${error}`,
    stack: error.stack,
  }, error), // passing custom properties e.g. `error.index`
});

// Message origin validation to prevent untrusted sources from sending messages
function isMessageTrusted(sender) {
  // Messages from the extension itself are always trusted
  if (sender.id === chrome.runtime.id) {
    return true;
  }
  
  // Messages without a tab (e.g., from extension pages) are trusted
  if (!sender.tab) {
    return true;
  }
  
  // Validate origin for messages from tabs
  if (sender.url) {
    try {
      const url = new URL(sender.url);
      // Allow messages from extension pages
      if (url.protocol === 'chrome-extension:' || url.protocol === 'moz-extension:') {
        return url.hostname === chrome.runtime.id || url.host === chrome.runtime.id;
      }
      // For other origins, only allow if explicitly from our extension's content scripts
      return sender.origin === `chrome-extension://${chrome.runtime.id}` ||
             sender.origin === `moz-extension://${chrome.runtime.id}`;
    } catch (e) {
      return false;
    }
  }
  
  return false;
}

chrome.runtime.onMessage.addListener(onRuntimeMessage);
if (__.ENTRY) {
  chrome.runtime.onConnect.addListener(async port => {
    if (__.IS_BG && global[k_busy]) await global[k_busy];
    const name = port.name.split(':', 1)[0];
    const fnOn = onConnect[name];
    const fnOff = onDisconnect[name];
    if (fnOn) fnOn(port);
    if (fnOff) port.onDisconnect.addListener(fnOff);
  });
}

export function _execute(data, sender, multi, broadcast) {
  let result;
  let res;
  let i = 0;
  if (__.ENTRY !== 'sw' && multi) {
    multi = data.length > 1 && data;
    data = data[0];
  }
  do {
    for (const [fn, replyAllowed] of onMessage) {
      try {
        data.broadcast = broadcast;
        res = fn(data, sender, !!multi);
      } catch (err) {
        res = Promise.reject(err);
      }
      if (replyAllowed && res !== result && result === undefined) {
        result = res;
      }
    }
  } while (__.ENTRY !== 'sw' && multi && (data = multi[++i]));
  return result;
}

function onRuntimeMessage({data, multi, TDM, broadcast}, sender, sendResponse) {
  // Validate message origin to prevent unauthorized access
  if (!isMessageTrusted(sender)) {
    console.warn('Rejected message from untrusted origin:', sender);
    return;
  }
  
  if (!__.MV3 && !__.IS_BG && data.method === 'backgroundReady') {
    bgReadySignal?.(true);
  }
  if (__.ENTRY === true && !__.IS_BG && data.method === kInvokeAPI) {
    return;
  }
  sender.TDM = TDM;
  let res = __.IS_BG && global[k_busy];
  res = res
    ? res.then(_execute.bind(null, data, sender, multi, broadcast))
    : _execute(data, sender, multi, broadcast);
  if (res instanceof Promise) {
    res.then(wrapData, wrapError).then(sendResponse);
    return true;
  }
  if (res !== undefined) sendResponse(wrapData(res));
}

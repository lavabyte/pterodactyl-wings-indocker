'use strict';

// ─── LOGGER ──────────────────────────────────────────────────────────────────
const LOG_LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
const LOG_LEVEL = LOG_LEVELS[process.env.LOG_LEVEL || 'debug'];

function timestamp() { return new Date().toISOString(); }

const log = {
  debug: (...a) => LOG_LEVEL <= 0 && console.log(`[${timestamp()}] [DEBUG]`, ...a),
  info:  (...a) => LOG_LEVEL <= 1 && console.log(`[${timestamp()}] [INFO] `, ...a),
  warn:  (...a) => LOG_LEVEL <= 2 && console.warn(`[${timestamp()}] [WARN] `, ...a),
  error: (...a) => LOG_LEVEL <= 3 && console.error(`[${timestamp()}] [ERROR]`, ...a),
};

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const CONFIG = {
  port: parseInt(process.env.PORT || '7860'),
  token: process.env.WINGS_TOKEN || 'changeme',
  jwtSecret: process.env.JWT_SECRET || 'changeme-jwt',
  serversDir: process.env.SERVERS_DIR || '/servers',
  imageCacheDir: process.env.IMAGE_CACHE_DIR || '/image-cache',
  dataDir: process.env.DATA_DIR || '/data',
  diskCheckInterval: 30000,
  remote: process.env.REMOTE_URL || '',
  tokenId: process.env.TOKEN_ID || '',
};

// ─── IMPORTS ─────────────────────────────────────────────────────────────────
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const archiver = require('archiver');
const pty = require('node-pty');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const { exec } = require('child_process');
const { promisify } = require('util');
const os = require('os');
const crypto = require('crypto');

const execAsync = promisify(exec);


// ─── PANEL CLIENT ─────────────────────────────────────────────────────────────
async function panelFetch(urlPath, method = 'GET', body = null) {
  if (!CONFIG.remote || !CONFIG.tokenId || !CONFIG.token) {
    throw new Error('REMOTE_URL / TOKEN_ID / WINGS_TOKEN not configured');
  }
  const https = require('https');
  const http = require('http');
  const url = new URL(urlPath, CONFIG.remote);
  const mod = url.protocol === 'https:' ? https : http;

  return new Promise((resolve, reject) => {
    const options = {
      hostname: url.hostname,
      port: url.port || (url.protocol === 'https:' ? 443 : 80),
      path: url.pathname + url.search,
      method,
      headers: {
        'Authorization': `Bearer ${CONFIG.tokenId}.${CONFIG.token}`,
        'Accept': 'application/vnd.pterodactyl.v1+json',
        'Content-Type': 'application/json',
        'User-Agent': 'Pterodactyl Wings/v1.11.13 (id:' + CONFIG.tokenId + ')',
      },
    };
    const req = mod.request(options, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        log.debug(`[panel] ${method} ${url.pathname} -> ${res.statusCode}`);
        if (res.statusCode >= 400) {
          reject(new Error(`Panel returned ${res.statusCode}: ${data.slice(0, 200)}`));
          return;
        }
        if (!data.trim()) { resolve(null); return; }
        try { resolve(JSON.parse(data)); } catch { resolve(data); }
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

async function fetchServerConfigFromPanel(uuid) {
  log.info(`[panel] Fetching server config for ${uuid}`);
  const data = await panelFetch(`/api/remote/servers/${uuid}`);
  log.debug(`[panel] Raw response for ${uuid}: ` + JSON.stringify(data).slice(0, 800));
  return data;
}

async function fetchInstallScript(uuid) {
  log.info(`[panel] Fetching install script for ${uuid}`);
  const data = await panelFetch(`/api/remote/servers/${uuid}/install`);
  log.debug(`[panel] Install script response: ` + JSON.stringify(data).slice(0, 400));
  return data; // { container_image, entrypoint, script }
}

async function notifyInstallComplete(uuid, successful) {
  try {
    await panelFetch(`/api/remote/servers/${uuid}/install`, 'POST', { successful });
    log.info(`[panel] Notified install complete for ${uuid}, successful=${successful}`);
  } catch (e) {
    log.warn(`[panel] Failed to notify install complete for ${uuid}: ${e.message}`);
  }
}

// ─── STATE ───────────────────────────────────────────────────────────────────
const SERVERS_FILE = path.join(CONFIG.dataDir, 'servers.json');

/** @type {Map<string, object>} uuid -> server config */
const serverConfigs = new Map();

/** @type {Map<string, object>} uuid -> process state */
const serverProcesses = new Map();

/** @type {Map<string, object>} download_id -> pull job */
const pullJobs = new Map();

/** @type {Map<string, Promise>} imageRef -> ongoing pull promise (dedup) */
const imagePullInProgress = new Map();

// ─── PERSISTENCE ─────────────────────────────────────────────────────────────
function loadServers() {
  try {
    if (fs.existsSync(SERVERS_FILE)) {
      const data = JSON.parse(fs.readFileSync(SERVERS_FILE, 'utf8'));
      for (const [uuid, cfg] of Object.entries(data)) serverConfigs.set(uuid, cfg);
    }
  } catch (e) { console.error('Failed to load servers.json:', e.message); }
}

function saveServers() {
  try {
    fs.writeFileSync(SERVERS_FILE, JSON.stringify(Object.fromEntries(serverConfigs), null, 2));
  } catch (e) { console.error('Failed to save servers.json:', e.message); }
}

// ─── HELPERS ─────────────────────────────────────────────────────────────────
function serverRootfsDir(uuid) { return path.join(CONFIG.serversDir, uuid); }
function serverAppDir(uuid)    { return path.join(CONFIG.serversDir, uuid, 'app'); }

function safePath(base, rel) {
  const resolved = path.resolve(base, rel.replace(/^\/+/, ''));
  if (!resolved.startsWith(base)) throw new Error('Path traversal detected');
  return resolved;
}

async function getDiskUsage(dir) {
  try {
    const { stdout } = await execAsync(`du -sb "${dir}"`);
    return parseInt(stdout.split('\t')[0]);
  } catch { return 0; }
}

function getProcessState(uuid) {
  const proc = serverProcesses.get(uuid);
  if (!proc) return 'offline';
  return proc.state || 'offline';
}

function buildResourcesPayload(uuid) {
  const proc = serverProcesses.get(uuid);
  const state = getProcessState(uuid);
  return {
    current_state: state,
    is_suspended: serverConfigs.get(uuid)?.suspended || false,
    resources: {
      memory_bytes: proc?.memBytes || 0,
      cpu_absolute: proc?.cpuAbsolute || 0,
      disk_bytes: proc?.diskBytes || 0,
      network_rx_bytes: 0,
      network_tx_bytes: 0,
      uptime: proc?.startedAt ? Date.now() - proc.startedAt : 0,
    },
  };
}

// ─── IMAGE PULLING ────────────────────────────────────────────────────────────
function parseImageRef(image) {
  let registry = 'registry-1.docker.io';
  let name = image;
  let tag = 'latest';

  const atIdx = name.lastIndexOf('@');
  const colonIdx = name.lastIndexOf(':');
  if (atIdx !== -1) {
    tag = name.slice(atIdx + 1);
    name = name.slice(0, atIdx);
  } else if (colonIdx !== -1 && !name.slice(colonIdx + 1).includes('/')) {
    tag = name.slice(colonIdx + 1);
    name = name.slice(0, colonIdx);
  }

  const parts = name.split('/');
  if (parts[0].includes('.') || parts[0].includes(':') || parts[0] === 'localhost') {
    registry = parts[0];
    name = parts.slice(1).join('/');
    if (registry === 'docker.io') registry = 'registry-1.docker.io';
  } else {
    if (parts.length === 1) name = `library/${name}`;
  }

  if (registry === 'registry-1.docker.io' && !name.includes('/')) {
    name = `library/${name}`;
  }

  const cacheKey = `${registry}/${name}:${tag}`.replace(/[^a-zA-Z0-9._\-:/]/g, '_');
  return { registry, name, tag, cacheKey };
}

function imageCachePath(cacheKey) {
  const safe = cacheKey.replace(/[/:]/g, '__');
  return path.join(CONFIG.imageCacheDir, safe);
}

async function pullImageToCache(image, logFn) {
  const { registry, name, tag, cacheKey } = parseImageRef(image);
  const dest = imageCachePath(cacheKey);

  log.info(`[image] pullImageToCache: ${image} -> cacheKey=${cacheKey} dest=${dest}`);
  if (fs.existsSync(path.join(dest, 'rootfs_ready'))) {
    logFn(`[image] Cache hit: ${cacheKey}`);
    log.info(`[image] Cache hit: ${cacheKey}`);
    return dest;
  }

  if (imagePullInProgress.has(cacheKey)) {
    logFn(`[image] Waiting for in-progress pull: ${cacheKey}`);
    await imagePullInProgress.get(cacheKey);
    return dest;
  }

  const pullPromise = (async () => {
    await fsp.mkdir(dest, { recursive: true });
    const rootfsDir = path.join(dest, 'rootfs');
    await fsp.mkdir(rootfsDir, { recursive: true });

    logFn(`[image] Pulling ${image} from ${registry}...`);

    const skopeoAvail = await execAsync('which skopeo').then(() => true).catch(() => false);
    if (skopeoAvail) {
      logFn(`[image] Using skopeo`);
      const ociDir = path.join(dest, 'oci');
      await execAsync(
        `skopeo copy --override-os linux --override-arch amd64 ` +
        `docker://${registry === 'registry-1.docker.io' ? '' : registry + '/'}${name}:${tag} ` +
        `oci:${ociDir}:${tag}`
      );
      const bundleDir = path.join(dest, 'bundle');
      await execAsync(`umoci unpack --rootless --image ${ociDir}:${tag} ${bundleDir}`);

      const bundleRootfs = path.join(bundleDir, 'rootfs');
      if (!fs.existsSync(bundleRootfs)) {
        const listing = await execAsync(`find ${bundleDir} -maxdepth 3 -type d 2>/dev/null || true`).then(r => r.stdout || r).catch(e => e.message);
        throw new Error(`umoci did not create bundle/rootfs. bundle contents: ${listing}`);
      }

      await execAsync(`cp -a "${bundleRootfs}/." "${rootfsDir}/"`);

      if (!fs.existsSync(path.join(rootfsDir, 'bin')) && !fs.existsSync(path.join(rootfsDir, 'usr'))) {
        const listing = await execAsync(`ls ${rootfsDir} 2>/dev/null || true`).then(r => r.stdout || r).catch(e => e.message);
        throw new Error(`cp -a of rootfs failed or rootfs is empty. destRootfs contents: ${listing}`);
      }

      // Extract PATH from OCI image config and save it for use at runtime
      try {
        const indexRaw = await execAsync(`cat "${ociDir}/index.json"`).then(r => r.stdout);
        const index = JSON.parse(indexRaw);
        const manifestDigest = index.manifests?.[0]?.digest?.replace(':', '/');
        if (manifestDigest) {
          const manifestRaw = await execAsync(`cat "${ociDir}/blobs/${manifestDigest}"`).then(r => r.stdout);
          const manifest = JSON.parse(manifestRaw);
          const configDigest = manifest.config?.digest?.replace(':', '/');
          if (configDigest) {
            const configJson = await execAsync(`cat "${ociDir}/blobs/${configDigest}"`).then(r => r.stdout);
            const imgConfig = JSON.parse(configJson);
            const envArr = imgConfig.config?.Env || [];
            const pathEntry = envArr.find(e => e.startsWith('PATH='));
            if (pathEntry) {
              fs.writeFileSync(path.join(dest, 'image_path'), pathEntry.slice(5));
              log.info(`[image] Saved PATH from image config: ${pathEntry.slice(5)}`);
            }
          }
        }
      } catch (e) {
        log.warn(`[image] Could not extract PATH from image config: ${e.message}`);
      }

      await execAsync(`rm -rf "${ociDir}" "${bundleDir}"`);
    } else {
      logFn(`[image] skopeo not found, using registry API`);
      await pullViaRegistryAPI({ registry, name, tag, dest: rootfsDir, logFn });
    }

    fs.writeFileSync(path.join(dest, 'rootfs_ready'), new Date().toISOString());
    logFn(`[image] Pull complete: ${cacheKey}`);
  })();

  imagePullInProgress.set(cacheKey, pullPromise);
  try {
    await pullPromise;
  } finally {
    imagePullInProgress.delete(cacheKey);
  }

  return dest;
}

async function pullViaRegistryAPI({ registry, name, tag, dest, logFn }) {
  const https = require('https');
  const zlib = require('zlib');

  const host = registry === 'registry-1.docker.io' ? 'registry-1.docker.io' : registry;

  async function fetchJSON(url, token) {
    return new Promise((resolve, reject) => {
      const headers = { 'Accept': 'application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.oci.image.index.v1+json' };
      if (token) headers['Authorization'] = `Bearer ${token}`;
      https.get(url, { headers }, (res) => {
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => {
          if (res.statusCode === 401) {
            resolve({ _status: 401, _headers: res.headers, _body: data });
            return;
          }
          try { resolve({ _status: res.statusCode, _headers: res.headers, ...JSON.parse(data) }); }
          catch { resolve({ _status: res.statusCode, _raw: data }); }
        });
        res.on('error', reject);
      }).on('error', reject);
    });
  }

  async function getToken(realm, service, scope) {
    const url = `${realm}?service=${encodeURIComponent(service)}&scope=${encodeURIComponent(scope)}`;
    const result = await fetchJSON(url, null);
    return result.token || result.access_token;
  }

  let token = null;
  let manifestUrl = `https://${host}/v2/${name}/manifests/${tag}`;
  let manifest = await fetchJSON(manifestUrl, null);

  if (manifest._status === 401) {
    const wwwAuth = manifest._headers['www-authenticate'] || '';
    const realmMatch = wwwAuth.match(/realm="([^"]+)"/);
    const serviceMatch = wwwAuth.match(/service="([^"]+)"/);
    const realm = realmMatch?.[1];
    const service = serviceMatch?.[1] || host;
    if (realm) {
      token = await getToken(realm, service, `repository:${name}:pull`);
      manifest = await fetchJSON(manifestUrl, token);
    }
  }

  const mediaType = manifest.mediaType || manifest._headers?.['content-type'] || '';
  if (mediaType.includes('manifest.list') || mediaType.includes('image.index') || manifest.manifests) {
    const amd64 = manifest.manifests?.find(m =>
      (!m.platform || (m.platform.architecture === 'amd64' && m.platform.os === 'linux'))
    );
    if (!amd64) throw new Error('No amd64/linux manifest found');
    manifestUrl = `https://${host}/v2/${name}/manifests/${amd64.digest}`;
    manifest = await fetchJSON(manifestUrl, token);
  }

  const layers = manifest.layers || manifest.fsLayers;
  if (!layers?.length) throw new Error(`No layers in manifest for ${name}:${tag}`);

  logFn(`[image] ${layers.length} layers to pull`);

  for (let i = 0; i < layers.length; i++) {
    const layer = layers[i];
    const digest = layer.digest || layer.blobSum;
    const layerUrl = `https://${host}/v2/${name}/blobs/${digest}`;
    logFn(`[image] Layer ${i + 1}/${layers.length}: ${digest.slice(0, 20)}...`);

    await new Promise((resolve, reject) => {
      const headers = {};
      if (token) headers['Authorization'] = `Bearer ${token}`;

      function doRequest(url, redirects) {
        if (redirects > 5) return reject(new Error('Too many redirects'));
        const mod = url.startsWith('https') ? https : require('http');
        mod.get(url, { headers }, (res) => {
          if (res.statusCode === 302 || res.statusCode === 301 || res.statusCode === 307) {
            return doRequest(res.headers.location, redirects + 1);
          }
          if (res.statusCode !== 200) {
            res.resume();
            return reject(new Error(`Layer fetch failed: ${res.statusCode} for ${url}`));
          }

          const gunzip = zlib.createGunzip();
          const tar = require('child_process').spawn('tar', ['-xf', '-', '-C', dest], { stdio: ['pipe', 'inherit', 'inherit'] });
          res.pipe(gunzip).pipe(tar.stdin);
          tar.on('close', code => code === 0 ? resolve() : reject(new Error(`tar exit ${code}`)));
          gunzip.on('error', e => { tar.kill(); reject(e); });
          res.on('error', reject);
        }).on('error', reject);
      }

      doRequest(layerUrl, 0);
    });
  }
}

// ─── IMAGE PATH RESOLUTION ───────────────────────────────────────────────────
// Returns the PATH that was saved from the image config during pull.
// Falls back to a broad default covering Debian/Ubuntu + common JVM paths.
function getImagePath(cacheKey) {
  if (cacheKey) {
    const imagePathFile = path.join(imageCachePath(cacheKey), 'image_path');
    if (fs.existsSync(imagePathFile)) {
      const p = fs.readFileSync(imagePathFile, 'utf8').trim();
      log.debug(`[image] Using PATH from image config: ${p}`);
      return p;
    }
  }
  // Broad fallback — covers most distros and common JVM install locations
  return [
    '/usr/local/sbin',
    '/usr/local/bin',
    '/usr/sbin',
    '/usr/bin',
    '/sbin',
    '/bin',
    '/usr/lib/jvm/java-21-openjdk-amd64/bin',
    '/usr/lib/jvm/java-21/bin',
    '/usr/lib/jvm/java-21-openjdk/bin',
    '/usr/local/lib/jvm/java-21-openjdk/bin',
    '/opt/java/21/bin',
    '/opt/jdk/21/bin',
  ].join(':');
}

// Copy cached rootfs to server directory
async function setupServerRootfs(uuid, image) {
  const { cacheKey } = parseImageRef(image);
  const cached = imageCachePath(cacheKey);

  const srcRootfs = path.join(cached, 'rootfs');
  const cacheReady = fs.existsSync(path.join(cached, 'rootfs_ready'));
  const cacheBin = fs.existsSync(path.join(srcRootfs, 'bin')) || fs.existsSync(path.join(srcRootfs, 'usr'));

  if (!cacheReady || !cacheBin) {
    log.warn(`[${uuid}] Cache invalid (ready=${cacheReady}, hasBin=${cacheBin}), re-pulling image`);
    await fsp.rm(cached, { recursive: true, force: true });
    await pullImageToCache(image, (msg) => log.info(`[${uuid}] ${msg}`));
  }

  if (!fs.existsSync(path.join(srcRootfs, 'bin')) && !fs.existsSync(path.join(srcRootfs, 'usr'))) {
    const listing = await execAsync(`ls "${srcRootfs}" 2>/dev/null || echo "(empty)"`).then(r => r.stdout || r).catch(() => '(error)');
    throw new Error(`Cached rootfs appears empty after pull attempt. Contents: ${listing}`);
  }

  const destRootfs = serverRootfsDir(uuid);
  const destApp = serverAppDir(uuid);

  await fsp.mkdir(destRootfs, { recursive: true });
  await fsp.mkdir(destApp, { recursive: true });

  log.info(`[${uuid}] Copying rootfs from ${srcRootfs} to ${destRootfs}`);
  await execAsync(`cp -a "${srcRootfs}/." "${destRootfs}/"`);

  if (!fs.existsSync(path.join(destRootfs, 'bin')) && !fs.existsSync(path.join(destRootfs, 'usr'))) {
    const listing = await execAsync(`ls "${destRootfs}" 2>/dev/null || echo "(empty)"`).then(r => r.stdout || r).catch(() => '(error)');
    throw new Error(`Failed to copy rootfs to server dir. destRootfs contents: ${listing}`);
  }

  await fsp.mkdir(path.join(destRootfs, 'app'), { recursive: true });
  log.info(`[${uuid}] rootfs setup complete`);
}

// ─── AUTH MIDDLEWARE ──────────────────────────────────────────────────────────
function authMiddleware(req, res, next) {
  const auth = req.headers.authorization;
  if (!auth || !auth.startsWith('Bearer ')) return res.status(401).json({ error: 'Unauthorized' });
  if (auth.slice(7) !== CONFIG.token) return res.status(401).json({ error: 'Unauthorized' });
  next();
}

// ─── SERVER PROCESS MANAGEMENT ───────────────────────────────────────────────
function startServer(uuid) {
  log.info(`[${uuid}] startServer called`);
  const cfg = serverConfigs.get(uuid);
  if (!cfg) throw new Error('Server not found');
  if (serverProcesses.get(uuid)?.state === 'running') throw new Error('Already running');
  log.debug(`[${uuid}] config: ` + JSON.stringify(cfg).slice(0, 400));

  const rootfsDir = serverRootfsDir(uuid);
  const appDir = serverAppDir(uuid);

  if (!fs.existsSync(rootfsDir) || (!fs.existsSync(path.join(rootfsDir, 'bin')) && !fs.existsSync(path.join(rootfsDir, 'usr')))) {
    throw new Error('Server rootfs not set up. Install the server first.');
  }

  fs.mkdirSync(appDir, { recursive: true });

  const rawCmd = cfg.invocation || cfg.container?.startup_command || cfg.startup_command || 'sh';
  const envVars = cfg.container?.environment || cfg.environment || {};

  // Expand {{VARIABLE}} placeholders
  const startCmd = rawCmd.replace(/\{\{([^}]+)\}\}/g, (_, key) => {
    return envVars[key] !== undefined ? envVars[key] : `{{${key}}}`;
  });
  log.info(`[${uuid}] invocation (raw): ${rawCmd}`);
  log.info(`[${uuid}] invocation (expanded): ${startCmd}`);

  // Get PATH from image config saved during pull
  const image = cfg.container?.image || cfg.image || '';
  const { cacheKey } = image ? parseImageRef(image) : { cacheKey: '' };
  const imagePath = getImagePath(cacheKey);
  log.info(`[${uuid}] PATH: ${imagePath}`);

  // Build env exports — PATH first so it applies to everything after
  const envExports = [
    `export PATH="${imagePath}"`,
    ...Object.entries(envVars).map(([k, v]) => `export ${k}=${JSON.stringify(String(v))}`),
  ].join('; ');

  const fullCmd = `${envExports}; ${startCmd}`;

  log.info(`[${uuid}] proot args: proot -R ${rootfsDir} -b ${appDir}:/app -w /app /bin/sh -c ${fullCmd.slice(0, 300)}`);

  const prootArgs = [
    '-R', rootfsDir,
    '-b', `${appDir}:/app`,
    '-w', '/app',
    '/bin/sh', '-c', fullCmd,
  ];

  let ptyProc;
  try {
    ptyProc = pty.spawn('proot', prootArgs, {
      name: 'xterm-color',
      cols: 200,
      rows: 50,
      cwd: appDir,
      env: { ...process.env, PATH: imagePath, ...envVars },
    });
  } catch (e) { throw new Error(`Failed to spawn proot: ${e.message}`); }

  const procState = {
    pty: ptyProc,
    state: 'running',
    clients: new Set(),
    memBytes: 0,
    cpuAbsolute: 0,
    diskBytes: 0,
    startedAt: Date.now(),
    logBuffer: [],
  };

  ptyProc.onData((data) => {
    const line = data.toString();
    procState.logBuffer.push(line);
    if (procState.logBuffer.length > 500) procState.logBuffer.shift();
    const msg = JSON.stringify({ event: 'console output', args: [line] });
    for (const ws of procState.clients) {
      if (ws.readyState === WebSocket.OPEN) ws.send(msg);
    }
  });

  ptyProc.onExit((e) => {
    log.info(`[${uuid}] process exited, code=${e?.exitCode} signal=${e?.signal}`);
    procState.state = 'offline';
    const msg = JSON.stringify({ event: 'console output', args: ['\r\n[Process exited]\r\n'] });
    for (const ws of procState.clients) {
      if (ws.readyState === WebSocket.OPEN) ws.send(msg);
    }
  });

  serverProcesses.set(uuid, procState);
}

function stopServer(uuid, kill = false) {
  log.info(`[${uuid}] stopServer called, kill=${kill}`);
  const proc = serverProcesses.get(uuid);
  if (!proc || proc.state === 'offline') { log.info(`[${uuid}] stopServer: already offline`); return; }
  proc.state = 'stopping';
  try {
    if (kill) {
      proc.pty.kill('SIGKILL');
    } else {
      proc.pty.write('stop\r');
      setTimeout(() => { if (proc.state !== 'offline') proc.pty.kill('SIGTERM'); }, 10000);
    }
  } catch (e) { console.error('Error stopping server:', e.message); }
}

// ─── INSTALL HANDLER ─────────────────────────────────────────────────────────
async function installServer(uuid, logFn) {
  log.info(`[${uuid}] installServer start`);
  let cfg = serverConfigs.get(uuid);
  if (!cfg) throw new Error('Server not found');

  // Fetch full server configuration from panel
  try {
    const panelData = await fetchServerConfigFromPanel(uuid);
    if (panelData) {
      const settings = panelData.settings || panelData;
      cfg = { ...cfg, ...settings };
      // Save process_configuration too (used for startup done detection etc.)
      if (panelData.process_configuration) {
        cfg._processConfig = panelData.process_configuration;
      }
      serverConfigs.set(uuid, cfg);
      saveServers();
      log.info(`[${uuid}] Merged panel config. Keys: ` + Object.keys(cfg).join(', '));
      log.debug(`[${uuid}] container after merge: ` + JSON.stringify(cfg.container || {}).slice(0, 500));
    }
  } catch (e) {
    log.warn(`[${uuid}] Could not fetch config from panel: ${e.message}`);
  }

  log.debug(`[${uuid}] full cfg keys: ` + Object.keys(cfg).join(', '));
  log.debug(`[${uuid}] container: ` + JSON.stringify(cfg.container || {}).slice(0, 500));
  const image = cfg.container?.image || cfg.image;
  log.info(`[${uuid}] resolved image: ${image}`);
  if (!image) {
    logFn(`[install] No image specified for ${uuid}, skipping rootfs setup`);
    await fsp.mkdir(serverAppDir(uuid), { recursive: true });
    await notifyInstallComplete(uuid, false);
    return;
  }

  logFn(`[install] Installing ${uuid} with image: ${image}`);

  // Pull server image to cache
  await pullImageToCache(image, logFn);

  // Remove old rootfs if reinstalling (keep /app)
  const rootfsDir = serverRootfsDir(uuid);
  if (fs.existsSync(rootfsDir)) {
    const entries = await fsp.readdir(rootfsDir);
    for (const e of entries) {
      if (e !== 'app') await fsp.rm(path.join(rootfsDir, e), { recursive: true, force: true });
    }
  }

  await setupServerRootfs(uuid, image);

  // Run egg install script (skip_egg_scripts=false means we SHOULD run it)
  const skipEggScripts = cfg.skip_egg_scripts === true;
  if (!skipEggScripts) {
    try {
      await runInstallScript(uuid, cfg, logFn);
    } catch (e) {
      log.warn(`[${uuid}] Install script failed: ${e.message}`);
      logFn(`[install] Install script error: ${e.message}`);
    }
  } else {
    logFn(`[install] Skipping egg scripts (skip_egg_scripts=true)`);
  }

  logFn(`[install] Done: ${uuid}`);
  await notifyInstallComplete(uuid, true);
}

async function runInstallScript(uuid, cfg, logFn) {
  // Fetch install script from panel
  let installScript;
  try {
    installScript = await fetchInstallScript(uuid);
  } catch (e) {
    log.warn(`[${uuid}] Could not fetch install script: ${e.message}`);
    logFn(`[install] Could not fetch install script: ${e.message}`);
    return;
  }

  if (!installScript || !installScript.script) {
    logFn(`[install] No install script returned from panel, skipping`);
    return;
  }

  const { script, entrypoint = '/bin/sh', container_image } = installScript;
  logFn(`[install] Running egg install script (entrypoint: ${entrypoint})`);
  log.info(`[${uuid}] Install script entrypoint: ${entrypoint}, image: ${container_image}`);
  log.debug(`[${uuid}] Install script (first 300 chars): ${script.slice(0, 300)}`);

  // Determine which rootfs to use for the install script.
  // Wings uses a separate install container image — we pull it if different from server image.
  const serverImage = cfg.container?.image || cfg.image || '';
  let installRootfs = serverRootfsDir(uuid);

  if (container_image && container_image !== serverImage) {
    logFn(`[install] Pulling install container image: ${container_image}`);
    try {
      const installCacheDest = await pullImageToCache(container_image, logFn);
      // Use a temp rootfs for the install script
      const installRootfsTemp = path.join(CONFIG.serversDir, uuid + '_install_tmp');
      const { cacheKey } = parseImageRef(container_image);
      const srcRootfs = path.join(imageCachePath(cacheKey), 'rootfs');
      await fsp.mkdir(installRootfsTemp, { recursive: true });
      await execAsync(`cp -a "${srcRootfs}/." "${installRootfsTemp}/"`);
      await fsp.mkdir(path.join(installRootfsTemp, 'mnt', 'server'), { recursive: true });
      installRootfs = installRootfsTemp;
    } catch (e) {
      logFn(`[install] Failed to pull install image, using server rootfs: ${e.message}`);
    }
  } else {
    await fsp.mkdir(path.join(installRootfs, 'mnt', 'server'), { recursive: true });
  }

  // Write install script to a temp file inside rootfs
  const scriptPath = path.join(installRootfs, 'tmp', 'install.sh');
  await fsp.mkdir(path.join(installRootfs, 'tmp'), { recursive: true });
  await fsp.writeFile(scriptPath, script, { mode: 0o755 });

  const appDir = serverAppDir(uuid);
  await fsp.mkdir(appDir, { recursive: true });

  // Build env from server environment
  const envVars = cfg.container?.environment || cfg.environment || {};
  const { cacheKey } = serverImage ? parseImageRef(serverImage) : { cacheKey: '' };
  const imagePath = getImagePath(cacheKey);

  const envExports = [
    `export PATH="${imagePath}"`,
    ...Object.entries(envVars).map(([k, v]) => `export ${k}=${JSON.stringify(String(v))}`),
  ].join('; ');

  // Run: proot -R <rootfs> -b <appDir>:/mnt/server -w /mnt/server <entrypoint> /tmp/install.sh
  const prootArgs = [
    '-R', installRootfs,
    '-b', `${appDir}:/mnt/server`,
    '-w', '/mnt/server',
    '/bin/sh', '-c', `${envExports}; ${entrypoint} /tmp/install.sh`,
  ];

  log.info(`[${uuid}] Running install script via proot`);
  logFn(`[install] Executing install script...`);

  await new Promise((resolve, reject) => {
    const proc = require('child_process').spawn('proot', prootArgs, {
      cwd: appDir,
      env: { ...process.env, PATH: imagePath, ...envVars },
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    proc.stdout.on('data', (d) => {
      const line = d.toString();
      logFn(`[install-script] ${line.trim()}`);
      log.debug(`[${uuid}][install-script] ${line.trim()}`);
    });
    proc.stderr.on('data', (d) => {
      const line = d.toString();
      logFn(`[install-script] ${line.trim()}`);
      log.debug(`[${uuid}][install-script] ${line.trim()}`);
    });
    proc.on('close', (code) => {
      log.info(`[${uuid}] Install script exited with code ${code}`);
      logFn(`[install] Install script exited with code ${code}`);
      // Clean up temp install rootfs if used
      if (installRootfs !== serverRootfsDir(uuid)) {
        fsp.rm(installRootfs, { recursive: true, force: true }).catch(() => {});
      }
      resolve(); // don't reject on non-zero — some install scripts exit non-zero but succeed
    });
    proc.on('error', reject);
  });
}

// ─── DISK CHECK ───────────────────────────────────────────────────────────────
async function diskCheck() {
  log.debug('diskCheck running');
  for (const [uuid, cfg] of serverConfigs) {
    const limitBytes = (cfg.build?.disk || 0) * 1024 * 1024;
    if (!limitBytes) continue;
    const dir = serverRootfsDir(uuid);
    const used = await getDiskUsage(dir);
    const proc = serverProcesses.get(uuid);
    if (proc) proc.diskBytes = used;
    if (used > limitBytes && proc?.state === 'running') {
      console.error(`[${uuid}] Disk limit exceeded (${used} > ${limitBytes}), killing`);
      stopServer(uuid, true);
    }
  }
}

setInterval(diskCheck, CONFIG.diskCheckInterval);

// ─── STATS COLLECTOR ─────────────────────────────────────────────────────────
async function collectStats() {
  for (const [uuid, proc] of serverProcesses) {
    if (proc.state !== 'running') continue;
    try {
      const pid = proc.pty.pid;
      const { stdout } = await execAsync(`ps -o rss= -p ${pid} 2>/dev/null || echo 0`);
      proc.memBytes = parseInt(stdout.trim()) * 1024 || 0;
    } catch { /* ignore */ }

    const statsMsg = JSON.stringify({
      event: 'stats',
      args: [JSON.stringify({
        memory_bytes: proc.memBytes,
        cpu_absolute: proc.cpuAbsolute,
        disk_bytes: proc.diskBytes,
        network_rx_bytes: 0,
        network_tx_bytes: 0,
        uptime: Date.now() - proc.startedAt,
      })],
    });
    for (const ws of proc.clients) {
      if (ws.readyState === WebSocket.OPEN) ws.send(statsMsg);
    }
  }
}

setInterval(collectStats, 5000);

// ─── EXPRESS APP ─────────────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '100mb' }));

// ─── HTTP LOGGING MIDDLEWARE ─────────────────────────────────────────────────
app.use((req, res, next) => {
  const start = Date.now();
  const bodySnippet = req.body && Object.keys(req.body).length
    ? ' body=' + JSON.stringify(req.body).slice(0, 500)
    : '';
  log.info(`--> ${req.method} ${req.url}${bodySnippet}`);
  const origJson = res.json.bind(res);
  res.json = (data) => {
    const snippet = JSON.stringify(data).slice(0, 300);
    log.info(`<-- ${req.method} ${req.url} ${res.statusCode} (${Date.now()-start}ms) ${snippet}`);
    return origJson(data);
  };
  const origEnd = res.end.bind(res);
  res.end = (...args) => {
    if (!res.headersSent || res.getHeader('content-type')?.includes('json') === false) {
      log.info(`<-- ${req.method} ${req.url} ${res.statusCode} (${Date.now()-start}ms)`);
    }
    return origEnd(...args);
  };
  next();
});

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 500 * 1024 * 1024 } });

// ─── SYSTEM ──────────────────────────────────────────────────────────────────
app.get('/api/system', authMiddleware, (req, res) => {
  res.json({
    architecture: process.arch === 'x64' ? 'amd64' : process.arch,
    cpu_count: os.cpus().length,
    kernel_version: os.release(),
    os: 'linux',
    version: '1.11.13',
  });
});

// ─── SERVERS CRUD ────────────────────────────────────────────────────────────
app.get('/api/servers', authMiddleware, (req, res) => {
  res.json([...serverConfigs.values()]);
});

app.post('/api/servers', authMiddleware, async (req, res) => {
  const cfg = req.body;
  log.info('POST /api/servers full payload: ' + JSON.stringify(cfg, null, 2));
  const uuid = cfg.uuid;
  if (!uuid) return res.status(400).json({ error: 'uuid required' });
  serverConfigs.set(uuid, cfg);
  saveServers();
  await fsp.mkdir(serverAppDir(uuid), { recursive: true });
  installServer(uuid, (msg) => console.log(`[${uuid}] ${msg}`)).catch(e =>
    console.error(`[${uuid}] Install failed:`, e.message)
  );
  res.status(204).end();
});

app.get('/api/servers/:uuid', authMiddleware, (req, res) => {
  const cfg = serverConfigs.get(req.params.uuid);
  if (!cfg) return res.status(404).json({ error: 'Not found' });
  res.json(cfg);
});

app.patch('/api/servers/:uuid', authMiddleware, (req, res) => {
  const { uuid } = req.params;
  if (!serverConfigs.has(uuid)) return res.status(404).json({ error: 'Not found' });
  serverConfigs.set(uuid, { ...serverConfigs.get(uuid), ...req.body });
  saveServers();
  res.status(204).end();
});

app.delete('/api/servers/:uuid', authMiddleware, async (req, res) => {
  const { uuid } = req.params;
  if (!serverConfigs.has(uuid)) return res.status(404).json({ error: 'Not found' });
  stopServer(uuid, true);
  serverProcesses.delete(uuid);
  serverConfigs.delete(uuid);
  saveServers();
  await fsp.rm(serverRootfsDir(uuid), { recursive: true, force: true }).catch(() => {});
  res.status(204).end();
});

app.post('/api/servers/:uuid/reinstall', authMiddleware, async (req, res) => {
  const { uuid } = req.params;
  if (!serverConfigs.has(uuid)) return res.status(404).json({ error: 'Not found' });
  stopServer(uuid, true);
  installServer(uuid, (msg) => console.log(`[${uuid}] ${msg}`)).catch(e =>
    console.error(`[${uuid}] Reinstall failed:`, e.message)
  );
  res.status(204).end();
});

// ─── POWER ───────────────────────────────────────────────────────────────────
app.post('/api/servers/:uuid/power', authMiddleware, (req, res) => {
  const { uuid } = req.params;
  const { action } = req.body;
  if (!serverConfigs.has(uuid)) return res.status(404).json({ error: 'Not found' });
  try {
    switch (action) {
      case 'start':   startServer(uuid); break;
      case 'stop':    stopServer(uuid, false); break;
      case 'kill':    stopServer(uuid, true); break;
      case 'restart':
        stopServer(uuid, false);
        setTimeout(() => { try { startServer(uuid); } catch (e) { console.error(e.message); } }, 3000);
        break;
      default: return res.status(400).json({ error: 'Invalid action' });
    }
    res.status(204).end();
  } catch (e) { res.status(409).json({ error: e.message }); }
});

// ─── COMMANDS ────────────────────────────────────────────────────────────────
app.post('/api/servers/:uuid/commands', authMiddleware, (req, res) => {
  const { uuid } = req.params;
  const { command } = req.body;
  const proc = serverProcesses.get(uuid);
  if (!proc || proc.state !== 'running') return res.status(409).json({ error: 'Server not running' });
  proc.pty.write(command + '\r');
  res.status(204).end();
});

// ─── RESOURCES ───────────────────────────────────────────────────────────────
app.get('/api/servers/:uuid/resources', authMiddleware, (req, res) => {
  const { uuid } = req.params;
  if (!serverConfigs.has(uuid)) return res.status(404).json({ error: 'Not found' });
  res.json(buildResourcesPayload(uuid));
});

// ─── FILES ───────────────────────────────────────────────────────────────────
async function handleFilesList(req, res) {
  const { uuid } = req.params;
  const base = serverAppDir(uuid);
  const rel = (req.query.directory || '/').toString();
  try {
    const target = safePath(base, rel);
    const entries = await fsp.readdir(target, { withFileTypes: true });
    const files = await Promise.all(entries.map(async (e) => {
      const full = path.join(target, e.name);
      let stat;
      try { stat = await fsp.stat(full); } catch { stat = { size: 0, mtimeMs: 0, mode: 0 }; }
      return {
        name: e.name,
        mode: stat.mode ? `0${(stat.mode & 0o777).toString(8)}` : '0644',
        size: stat.size,
        is_file: e.isFile(),
        is_symlink: e.isSymbolicLink(),
        is_editable: e.isFile(),
        mimetype: e.isFile() ? 'text/plain' : 'inode/directory',
        created_at: new Date(stat.birthtimeMs || stat.ctimeMs || 0).toISOString(),
        modified_at: new Date(stat.mtimeMs).toISOString(),
      };
    }));
    res.json(files);
  } catch (e) { res.status(500).json({ error: e.message }); }
}
app.get('/api/servers/:uuid/files/list', authMiddleware, handleFilesList);
app.get('/api/servers/:uuid/files/list-directory', authMiddleware, handleFilesList);

app.get('/api/servers/:uuid/files/contents', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  try {
    const target = safePath(base, (req.query.file || '').toString());
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    fs.createReadStream(target).pipe(res);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/servers/:uuid/files/download', authMiddleware, async (req, res) => {
  const { uuid } = req.params;
  const rel = (req.query.file || '').toString();
  const url = `${req.protocol}://${req.get('host')}/api/servers/${uuid}/files/contents?file=${encodeURIComponent(rel)}`;
  res.json({ url });
});

app.put('/api/servers/:uuid/files/rename', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  const { root, files } = req.body;
  try {
    for (const f of files) {
      await fsp.rename(safePath(base, path.join(root || '/', f.from)), safePath(base, path.join(root || '/', f.to)));
    }
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:uuid/files/copy', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  try {
    const src = safePath(base, req.body.location);
    await execAsync(`cp -r "${src}" "${src}_copy_${Date.now()}"`);
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:uuid/files/write', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  const rel = (req.query.file || '').toString();
  try {
    const target = safePath(base, rel);
    await fsp.mkdir(path.dirname(target), { recursive: true });
    let body = '';
    req.setEncoding('utf8');
    req.on('data', c => { body += c; });
    req.on('end', async () => { await fsp.writeFile(target, body, 'utf8'); res.status(204).end(); });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:uuid/files/delete', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  const { root, files } = req.body;
  try {
    for (const f of files) await fsp.rm(safePath(base, path.join(root || '/', f)), { recursive: true, force: true });
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:uuid/files/create-folder', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  try {
    await fsp.mkdir(safePath(base, path.join(req.body.root || '/', req.body.name)), { recursive: true });
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:uuid/files/create-directory', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  try {
    await fsp.mkdir(safePath(base, path.join(req.body.root || '/', req.body.name)), { recursive: true });
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:uuid/files/compress', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  const { root, files } = req.body;
  try {
    const archiveName = `archive_${Date.now()}.tar.gz`;
    const archivePath = safePath(base, path.join(root || '/', archiveName));
    const fileArgs = files.map(f => `"${safePath(base, path.join(root || '/', f))}"`).join(' ');
    await execAsync(`tar -czf "${archivePath}" -C "${base}" ${fileArgs}`);
    res.json({ name: archiveName });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:uuid/files/decompress', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  try {
    const src = safePath(base, path.join(req.body.root || '/', req.body.file));
    const destDir = safePath(base, req.body.root || '/');
    await execAsync(`tar -xzf "${src}" -C "${destDir}"`);
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:uuid/files/pull', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  const { url, directory } = req.body;
  const dlId = crypto.randomUUID();
  const destDir = safePath(base, directory || '/');
  const fileName = path.basename(new URL(url).pathname) || `download_${dlId}`;
  const destPath = path.join(destDir, fileName);

  const job = { downloaded: 0, total: 0, complete: false, error: null };
  pullJobs.set(dlId, job);

  (async () => {
    try {
      await execAsync(`curl -L -o "${destPath}" "${url}"`);
      job.complete = true;
    } catch (e) { job.error = e.message; }
  })();

  res.json({ download_id: dlId });
});

app.get('/api/servers/:uuid/files/pull/:download_id', authMiddleware, (req, res) => {
  const job = pullJobs.get(req.params.download_id);
  if (!job) return res.status(404).json({ error: 'Not found' });
  res.json(job);
});

app.delete('/api/servers/:uuid/files/pull/:download_id', authMiddleware, (req, res) => {
  pullJobs.delete(req.params.download_id);
  res.status(204).end();
});

app.post('/api/servers/:uuid/files/upload', authMiddleware, upload.single('files'), async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  try {
    const target = safePath(base, (req.query.directory || '/').toString());
    await fsp.mkdir(target, { recursive: true });
    if (req.file) await fsp.writeFile(path.join(target, req.file.originalname), req.file.buffer);
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ─── BACKUPS ─────────────────────────────────────────────────────────────────
app.post('/api/servers/:uuid/backup', authMiddleware, async (req, res) => {
  const { uuid } = req.params;
  const backupId = crypto.randomUUID();
  const backupPath = path.join(CONFIG.dataDir, 'backups', uuid);
  await fsp.mkdir(backupPath, { recursive: true });
  const dest = path.join(backupPath, `${backupId}.tar.gz`);
  execAsync(`tar -czf "${dest}" -C "${serverRootfsDir(uuid)}" app`).catch(e =>
    console.error('Backup failed:', e.message)
  );
  res.json({
    uuid: backupId, is_successful: false, checksum: '', checksum_type: 'sha1',
    bytes: 0, created_at: new Date().toISOString(), parts: [],
  });
});

app.get('/api/servers/:uuid/backup/:backup/download', authMiddleware, (req, res) => {
  res.json({ url: `${req.protocol}://${req.get('host')}/api/servers/${req.params.uuid}/backup/${req.params.backup}/file` });
});

app.delete('/api/servers/:uuid/backup/:backup', authMiddleware, async (req, res) => {
  await fsp.rm(path.join(CONFIG.dataDir, 'backups', req.params.uuid, `${req.params.backup}.tar.gz`), { force: true });
  res.status(204).end();
});

// ─── NETWORK / TRANSFERS / SFTP ──────────────────────────────────────────────
app.get('/api/servers/:uuid/network', authMiddleware, (req, res) => res.json({ interfaces: [] }));
app.post('/api/servers/:uuid/network', authMiddleware, (req, res) => res.json({ interface: req.body }));
app.delete('/api/servers/:uuid/network', authMiddleware, (req, res) => res.status(204).end());
app.post('/api/transfers', authMiddleware, (req, res) => res.status(501).json({ error: 'Transfers not supported' }));
app.delete('/api/transfers/:uuid', authMiddleware, (req, res) => res.status(204).end());
app.post('/api/sftp/auth', authMiddleware, (req, res) => res.status(501).json({ error: 'SFTP not supported' }));

app.post('/api/servers/:uuid/files/chmod', authMiddleware, async (req, res) => {
  const base = serverAppDir(req.params.uuid);
  const { root, files } = req.body;
  try {
    for (const f of (files || [])) {
      const target = safePath(base, path.join(root || '/', f.file));
      await fsp.chmod(target, f.mode);
    }
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/servers/:uuid/logs', authMiddleware, (req, res) => {
  const { uuid } = req.params;
  if (!serverConfigs.has(uuid)) return res.status(404).json({ error: 'Not found' });
  const proc = serverProcesses.get(uuid);
  const lines = proc ? proc.logBuffer.slice(-100) : [];
  res.json({ data: lines.join('\n') });
});

app.post('/api/servers/:uuid/install', authMiddleware, async (req, res) => {
  const { uuid } = req.params;
  if (!serverConfigs.has(uuid)) return res.status(404).json({ error: 'Not found' });
  res.status(204).end();
});

app.post('/api/servers/:uuid/sync', authMiddleware, async (req, res) => {
  const { uuid } = req.params;
  if (!serverConfigs.has(uuid)) return res.status(404).json({ error: 'Not found' });
  try {
    const data = await fetchServerConfigFromPanel(uuid);
    if (data && data.settings) {
      const merged = { ...serverConfigs.get(uuid), ...data.settings };
      serverConfigs.set(uuid, merged);
      saveServers();
    }
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/servers/:uuid/ws/deny', authMiddleware, (req, res) => res.status(204).end());
app.post('/api/servers/:uuid/transfer', authMiddleware, (req, res) => res.status(501).json({ error: 'Transfers not supported' }));
app.delete('/api/servers/:uuid/transfer', authMiddleware, (req, res) => res.status(204).end());
app.post('/api/update', authMiddleware, (req, res) => res.status(204).end());
app.post('/api/deauthorize-user', authMiddleware, (req, res) => res.status(204).end());

app.post('/api/servers/:uuid/backup/:backup/restore', authMiddleware, async (req, res) => {
  const { uuid, backup } = req.params;
  if (!serverConfigs.has(uuid)) return res.status(404).json({ error: 'Not found' });
  const backupFile = path.join(CONFIG.dataDir, 'backups', uuid, `${backup}.tar.gz`);
  try {
    await fsp.access(backupFile);
    const appDir = serverAppDir(uuid);
    await execAsync(`tar -xzf "${backupFile}" -C "${appDir}"`);
    res.status(204).end();
  } catch (e) { res.status(500).json({ error: e.message }); }
});

const downloadTokens = new Map();

app.get('/download/backup', async (req, res) => {
  const token = req.query.token;
  const entry = downloadTokens.get(token);
  if (!entry || entry.type !== 'backup' || Date.now() > entry.expires) return res.status(403).json({ error: 'Invalid token' });
  downloadTokens.delete(token);
  const filePath = path.join(CONFIG.dataDir, 'backups', entry.uuid, `${entry.path}.tar.gz`);
  try {
    await fsp.access(filePath);
    res.download(filePath);
  } catch (e) { res.status(404).json({ error: 'Backup not found' }); }
});

app.get('/download/file', async (req, res) => {
  const token = req.query.token;
  const entry = downloadTokens.get(token);
  if (!entry || entry.type !== 'file' || Date.now() > entry.expires) return res.status(403).json({ error: 'Invalid token' });
  downloadTokens.delete(token);
  const base = serverAppDir(entry.uuid);
  try {
    const filePath = safePath(base, entry.path);
    res.download(filePath);
  } catch (e) { res.status(404).json({ error: 'File not found' }); }
});

// ─── WEBSOCKET ───────────────────────────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const urlObj = new URL(req.url, `http://${req.headers.host}`);
  const match = urlObj.pathname.match(/^\/api\/servers\/([^/]+)\/ws$/);
  if (!match) { socket.destroy(); return; }
  const uuid = match[1];
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req, uuid));
});

wss.on('connection', (ws, req, uuid) => {
  log.info(`[WS] New connection for server ${uuid}`);
  if (!serverConfigs.has(uuid)) {
    log.warn(`[WS] Server ${uuid} not found, closing`);
    ws.close(4004, 'Server not found');
    return;
  }

  let authed = false;
  const sendJson = (obj) => { try { ws.send(JSON.stringify(obj)); } catch {} };

  const proc = serverProcesses.get(uuid);
  if (proc) proc.clients.add(ws);

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      log.debug(`[WS][${uuid}] event=${msg.event} args=${JSON.stringify(msg.args||[]).slice(0,120)}`);

      if (msg.event === 'auth') {
        const token = (msg.args || [])[0] || '';
        let valid = token === CONFIG.token;
        if (!valid) {
          try { jwt.verify(token, CONFIG.jwtSecret); valid = true; } catch {}
        }
        if (!valid) {
          sendJson({ event: 'jwt error', args: ['jwt: authentication token has expired'] });
          return;
        }
        authed = true;
        sendJson({ event: 'auth success', args: [] });
        sendJson({ event: 'status', args: [getProcessState(uuid)] });
        sendJson({ event: 'stats', args: [JSON.stringify(buildResourcesPayload(uuid))] });
        const p = serverProcesses.get(uuid);
        if (p) {
          for (const line of p.logBuffer.slice(-50)) {
            sendJson({ event: 'console output', args: [line] });
          }
        }
        return;
      }

      if (!authed) {
        sendJson({ event: 'jwt error', args: ['jwt: no jwt present'] });
        return;
      }

      if (msg.event === 'send command') {
        const p = serverProcesses.get(uuid);
        if (p?.state === 'running') p.pty.write((msg.args[0] || '') + '\r');
        return;
      }

      if (msg.event === 'set state') {
        const action = (msg.args || [])[0];
        try {
          if (action === 'start') startServer(uuid);
          else if (action === 'stop') stopServer(uuid, false);
          else if (action === 'kill') stopServer(uuid, true);
          else if (action === 'restart') {
            stopServer(uuid, false);
            setTimeout(() => { try { startServer(uuid); } catch {} }, 3000);
          }
        } catch (e) {
          sendJson({ event: 'daemon message', args: [`Error: ${e.message}`] });
        }
        return;
      }

      if (msg.event === 'send logs') {
        const p = serverProcesses.get(uuid);
        const lines = p ? p.logBuffer.slice(-100) : [];
        for (const line of lines) sendJson({ event: 'console output', args: [line] });
        return;
      }

      if (msg.event === 'send stats') {
        sendJson({ event: 'stats', args: [JSON.stringify(buildResourcesPayload(uuid))] });
        return;
      }

    } catch (e) {
      log.debug(`[WS][${uuid}] parse error: ${e.message}`);
    }
  });

  ws.on('close', () => {
    const p = serverProcesses.get(uuid);
    if (p) p.clients.delete(ws);
    log.debug(`[WS][${uuid}] connection closed`);
  });
});

// ─── BOOT ────────────────────────────────────────────────────────────────────
fs.mkdirSync(CONFIG.dataDir, { recursive: true });
fs.mkdirSync(CONFIG.serversDir, { recursive: true });
fs.mkdirSync(CONFIG.imageCacheDir, { recursive: true });
fs.mkdirSync(path.join(CONFIG.dataDir, 'backups'), { recursive: true });
loadServers();

server.listen(CONFIG.port, '0.0.0.0', () => {
  log.info(`Wings daemon listening on :${CONFIG.port}`);
  log.info(`Token: ${CONFIG.token}`);
  log.info(`Servers dir: ${CONFIG.serversDir}`);
  log.info(`Image cache dir: ${CONFIG.imageCacheDir}`);
  log.info(`Data dir: ${CONFIG.dataDir}`);
  log.info(`Remote panel URL: ${CONFIG.remote || '(not set)'}`);
  log.info(`Token ID: ${CONFIG.tokenId || '(not set)'}`);
  log.info(`Loaded servers: ${[...serverConfigs.keys()].join(', ') || '(none)'}`);
});

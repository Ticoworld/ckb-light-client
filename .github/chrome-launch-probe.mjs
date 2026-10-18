import { spawn, execFileSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { runInNewContext } from 'node:vm';

const output = process.argv[2] ?? 'chrome-probe-results';
const expectSandbox = process.argv[3] === 'sandbox-available';
await mkdir(output);
const source = await readFile('wasm/light-client-js/tests/browser/run.mjs', 'utf8');
const launch = source.match(/browser=spawn\(executable,(\[[\s\S]*?\]),\s*\{windowsHide/);
if (!launch) throw new Error('Could not extract actual runner arguments');
const results = [];
for (const [name, extra] of [
  ['baseline', []],
  ['shared-memory-only', ['--disable-dev-shm-usage']],
  ['sandbox-only', ['--no-sandbox']],
  ['webdriver-options', ['--disable-dev-shm-usage', '--no-sandbox']],
  ['baseline-repeat', []],
  ['sandbox-only-repeat', ['--no-sandbox']],
]) {
  const profile = await mkdtemp(join(tmpdir(), 'ckb-chrome-probe-'));
  const args = runInNewContext(`(${launch[1]})`, { profile, process: { env: {} } });
  args.splice(args.length - 1, 0, ...extra);
  const result = { name, args, executable: process.env.CHROME, stderr: '', endpoint: false };
  const child = spawn(process.env.CHROME, args, { stdio: ['ignore', 'ignore', 'pipe'] });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => { result.timeout = true; child.kill(); }, 15000);
    child.on('error', reject);
    child.stderr.on('data', chunk => {
      result.stderr = (result.stderr + chunk).slice(-32768);
      if (!result.endpoint && /DevTools listening on ws:\/\//.test(result.stderr)) {
        result.endpoint = true;
        child.kill();
      }
    });
    child.once('close', (code, signal) => {
      clearTimeout(timer);
      Object.assign(result, { code, signal });
      resolve();
    });
  });
  results.push(result);
  console.log(JSON.stringify(result, null, 2));
  await writeFile(join(output, `${name}.json`), JSON.stringify(result, null, 2));
  await rm(profile, { recursive: true, force: true });
}

// Ask the same ChromeDriver binary to launch with wasm-bindgen 0.2.100's options.
const driver = spawn(process.env.CHROMEDRIVER, ['--port=9515', '--verbose', `--log-path=${output}/chromedriver.log`], { stdio: ['ignore', 'pipe', 'pipe'] });
let driverOutput = '';
driver.stdout.on('data', c => { driverOutput += c; });
driver.stderr.on('data', c => { driverOutput += c; });
try {
  let ready = false;
  for (let i = 0; i < 100; i++) {
    try { if ((await fetch('http://127.0.0.1:9515/status')).ok) { ready = true; break; } } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  if (!ready) throw new Error('ChromeDriver failed to start: ' + driverOutput);
  const response = await fetch('http://127.0.0.1:9515/session', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ capabilities: { alwaysMatch: { browserName: 'chrome', 'goog:chromeOptions': {
      binary: process.env.CHROME, args: ['headless', 'disable-dev-shm-usage', 'no-sandbox'],
    } } } }),
  });
  const session = await response.json();
  await writeFile(join(output, 'chromedriver-session.json'), JSON.stringify(session, null, 2));
  if (!response.ok || !session.value?.sessionId) throw new Error(JSON.stringify(session));
  console.log('ChromeDriver session succeeded', session.value.capabilities.browserVersion);
  await fetch(`http://127.0.0.1:9515/session/${session.value.sessionId}`, { method: 'DELETE' });
} finally { driver.kill(); }

const summary = { chrome: execFileSync(process.env.CHROME, ['--version'], { encoding: 'utf8' }).trim(), results };
await writeFile(join(output, 'summary.json'), JSON.stringify(summary, null, 2));
if (results.filter(r => r.name.startsWith('baseline')).some(r => r.endpoint !== expectSandbox)) throw new Error('Unexpected sandbox control result');
if (results.filter(r => r.name.startsWith('sandbox-only')).some(r => !r.endpoint)) throw new Error('Sandbox-only control did not start');

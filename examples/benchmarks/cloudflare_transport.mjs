/** Synthetic benchmark transport. Credential and request travel through stdin. */
let input = '';
for await (const chunk of process.stdin) input += chunk;
const { account_id: account, token, path, body, multipart } = JSON.parse(input);
const headers = { Authorization: `Bearer ${token}` };
let payload;
if (multipart) {
  payload = new FormData();
  payload.append('vectors', new Blob([body], { type: 'application/x-ndjson' }), 'vectors.ndjson');
} else if (body !== null) {
  headers['Content-Type'] = 'application/json';
  payload = JSON.stringify(body);
}
try {
  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${encodeURIComponent(account)}${path}`,
    { method: body === null ? 'GET' : 'POST', headers, body: payload,
      signal: AbortSignal.timeout(60000) },
  );
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const envelope = await response.json();
  if (envelope.success !== true) throw new Error('request rejected');
  process.stdout.write(JSON.stringify(envelope.result));
} catch (error) {
  // No token, URL, account, body or server diagnostic in the error stream.
  process.stderr.write(/^HTTP \d+$/.test(error.message) ? error.message : 'transport unavailable');
  process.exitCode = 1;
}

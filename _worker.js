import { connect } from 'cloudflare:sockets';

const UUID = '1ff6954f-e1c7-4fca-96f9-1d01985c938a';
const PROXY_IP = 'ProxyIP.US.CMLiussss.net';

// CDN优选节点，按地区分组，格式: 域名:端口#标签（端口默认443）
const NODES = [
	// 香港
	'saas.sin.fan#HK-1',
	'cf.008500.xyz#HK-2',
	'cf.877774.xyz#HK-3',
	'cf.zhetengsha.eu.org#HK-4',
	'www.visa.com.hk#HK-5',
	// 日本
	'store.ubi.com#JP-1',
	'cdns.doon.eu.org#JP-2',
	// 韩国
	'cf.130519.xyz#KR-1',
	// 新加坡
	'mfa.gov.ua#SG-1',
	'cf.090227.xyz#SG-2',
	'www.visa.com.sg#SG-3',
	// 台湾
	'sub.danfeng.eu.org#TW-1',
	// 欧美
	'www.gov.se#EU-1',
	'shopify.com#US-1',
	'time.is#US-2',
];

function closeWs(ws) {
	try { if (ws.readyState <= 1) ws.close(); } catch {}
}

function bytesToUuid(arr, off = 0) {
	const h = [...arr.slice(off, off + 16)].map(b => b.toString(16).padStart(2, '0')).join('');
	return `${h.slice(0,8)}-${h.slice(8,12)}-${h.slice(12,16)}-${h.slice(16,20)}-${h.slice(20)}`;
}

function parseVless(buf) {
	if (buf.byteLength < 24) return { error: 'too short' };
	const version = new Uint8Array(buf.slice(0, 1));
	if (bytesToUuid(new Uint8Array(buf.slice(1, 17))) !== UUID) return { error: 'bad uuid' };
	const optLen = new Uint8Array(buf.slice(17, 18))[0];
	const cmd = new Uint8Array(buf.slice(18 + optLen, 19 + optLen))[0];
	if (cmd !== 1 && cmd !== 2) return { error: 'bad cmd' };
	const pIdx = 19 + optLen;
	const port = new DataView(buf.slice(pIdx, pIdx + 2)).getUint16(0);
	const aIdx = pIdx + 2;
	const aType = new Uint8Array(buf.slice(aIdx, aIdx + 1))[0];
	let host = '', vIdx = aIdx + 1, aLen = 0;
	if (aType === 1) {
		aLen = 4;
		host = new Uint8Array(buf.slice(vIdx, vIdx + 4)).join('.');
	} else if (aType === 2) {
		aLen = new Uint8Array(buf.slice(vIdx, vIdx + 1))[0];
		vIdx++;
		host = new TextDecoder().decode(buf.slice(vIdx, vIdx + aLen));
	} else if (aType === 3) {
		aLen = 16;
		const v = new DataView(buf.slice(vIdx, vIdx + 16));
		host = Array.from({ length: 8 }, (_, i) => v.getUint16(i * 2).toString(16)).join(':');
	} else return { error: 'bad addr' };
	return { version, port, host, isUDP: cmd === 2, dataOffset: vIdx + aLen };
}

function wsReadable(ws, earlyB64) {
	let done = false;
	return new ReadableStream({
		start(ctrl) {
			ws.addEventListener('message', e => { if (!done) ctrl.enqueue(e.data); });
			ws.addEventListener('close', () => { if (!done) { done = true; closeWs(ws); ctrl.close(); } });
			ws.addEventListener('error', e => ctrl.error(e));
			if (earlyB64) {
				try {
					const bin = atob(earlyB64.replace(/-/g, '+').replace(/_/g, '/'));
					const bytes = new Uint8Array(bin.length);
					for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
					ctrl.enqueue(bytes.buffer);
				} catch (e) { ctrl.error(e); }
			}
		},
		cancel() { done = true; closeWs(ws); }
	});
}

async function pipeToWs(remote, ws, header) {
	let h = header, hasData = false;
	await remote.readable.pipeTo(new WritableStream({
		write(chunk) {
			hasData = true;
			if (ws.readyState !== WebSocket.OPEN) throw new Error('ws closed');
			if (h) {
				const m = new Uint8Array(h.length + chunk.byteLength);
				m.set(h, 0);
				m.set(new Uint8Array(chunk), h.length);
				ws.send(m.buffer);
				h = null;
			} else {
				ws.send(chunk);
			}
		}
	})).catch(() => closeWs(ws));
	return hasData;
}

async function tcpForward(host, port, data, ws, respHeader, connRef) {
	async function dial(h, p, d) {
		const sock = connect({ hostname: h, port: p });
		const w = sock.writable.getWriter();
		await w.write(d);
		w.releaseLock();
		return sock;
	}
	async function viaProxy() {
		const sock = await dial(PROXY_IP, 443, data);
		connRef.socket = sock;
		sock.closed.catch(() => {}).finally(() => closeWs(ws));
		await pipeToWs(sock, ws, respHeader);
	}
	try {
		const sock = await dial(host, port, data);
		connRef.socket = sock;
		const ok = await pipeToWs(sock, ws, respHeader);
		if (!ok) await viaProxy();
	} catch {
		await viaProxy();
	}
}

async function dnsForward(chunk, ws, header) {
	const sock = connect({ hostname: '8.8.4.4', port: 53 });
	const w = sock.writable.getWriter();
	await w.write(chunk);
	w.releaseLock();
	let h = header;
	await sock.readable.pipeTo(new WritableStream({
		write(chunk) {
			if (ws.readyState !== WebSocket.OPEN) return;
			if (h) {
				const m = new Uint8Array(h.length + chunk.byteLength);
				m.set(h, 0);
				m.set(new Uint8Array(chunk), h.length);
				ws.send(m.buffer);
				h = null;
			} else {
				ws.send(chunk);
			}
		}
	})).catch(() => {});
}

async function handleWs(request) {
	const [client, server] = Object.values(new WebSocketPair());
	server.accept();
	const connRef = { socket: null };
	let isDns = false;
	const earlyData = request.headers.get('sec-websocket-protocol') || '';

	wsReadable(server, earlyData).pipeTo(new WritableStream({
		async write(chunk) {
			if (isDns) return dnsForward(chunk, server, null);
			if (connRef.socket) {
				const w = connRef.socket.writable.getWriter();
				await w.write(chunk);
				w.releaseLock();
				return;
			}
			const p = parseVless(chunk);
			if (p.error) throw new Error(p.error);
			if (p.isUDP && p.port !== 53) throw new Error('UDP not supported');
			if (p.isUDP) isDns = true;
			const header = new Uint8Array([p.version[0], 0]);
			const raw = chunk.slice(p.dataOffset);
			if (isDns) return dnsForward(raw, server, header);
			await tcpForward(p.host, p.port, raw, server, header, connRef);
		}
	})).catch(() => {});

	return new Response(null, { status: 101, webSocket: client });
}

function subscription(domain) {
	const links = NODES.map(n => {
		const [hp, tag] = n.split('#');
		const [host, port = '443'] = hp.includes(':') ? hp.split(':') : [hp];
		return `vless://${UUID}@${host}:${port}?encryption=none&security=tls&sni=${domain}&fp=firefox&allowInsecure=0&type=ws&host=${domain}&path=%2F%3Fed%3D2560#${tag}`;
	});
	return new Response(btoa(links.join('\n')), {
		headers: { 'Content-Type': 'text/plain;charset=utf-8', 'Cache-Control': 'no-store' }
	});
}

export default {
	async fetch(request) {
		try {
			const url = new URL(request.url);
			if (request.headers.get('Upgrade') === 'websocket') return handleWs(request);
			if (url.pathname === `/${UUID}`) return subscription(url.hostname);
			if (url.pathname === '/') return new Response('OK', { status: 200 });
			return new Response('Not Found', { status: 404 });
		} catch {
			return new Response('Error', { status: 500 });
		}
	}
};

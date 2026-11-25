// Calico Kombat 3D: servidor de relevo para multijugador (Cloudflare Workers + Durable Objects)
// Cada sala es un Durable Object. El anfitrión manda en la partida; este servidor solo reenvía mensajes.
export class Room {
  constructor(state) { this.state = state; }
  info(ws) { try { return ws.deserializeAttachment() || {}; } catch (e) { return {}; } }
  host() { return this.state.getWebSockets('host')[0]; }
  guests() { return this.state.getWebSockets('guest'); }
  async fetch(req) {
    if (req.headers.get('Upgrade') !== 'websocket') return new Response('se esperaba websocket', { status: 426 });
    const role = new URL(req.url).searchParams.get('role');
    const [client, server] = Object.values(new WebSocketPair());
    const host = this.host();
    const reject = (ev) => {
      this.state.acceptWebSocket(server, ['reject']); server.serializeAttachment({ role: 'reject' });
      server.send(JSON.stringify({ ev })); server.close(4000, ev);
    };
    if (role === 'host') {
      if (host) reject('taken');
      else { this.state.acceptWebSocket(server, ['host']); server.serializeAttachment({ role: 'host' }); server.send('{"ev":"ok"}'); }
    } else {
      const gs = this.guests();
      if (!host) reject('nohost');
      else if (gs.length >= 3) reject('full');
      else {
        const used = new Set(gs.map(w => this.info(w).id)); let id = 1; while (used.has(id)) id++;
        this.state.acceptWebSocket(server, ['guest', 'g' + id]); server.serializeAttachment({ role: 'guest', id });
        server.send('{"ev":"ok"}'); host.send(JSON.stringify({ ev: 'join', id }));
      }
    }
    return new Response(null, { status: 101, webSocket: client });
  }
  webSocketMessage(ws, msg) {
    const me = this.info(ws);
    if (me.role === 'host') {
      let m; try { m = JSON.parse(msg); } catch (e) { return; }
      const out = JSON.stringify(m.d);
      if (m.to === 'all') for (const g of this.guests()) { try { g.send(out); } catch (e) {} }
      else { const g = this.state.getWebSockets('g' + m.to)[0]; if (g) try { g.send(out); } catch (e) {} }
    } else if (me.role === 'guest') {
      const h = this.host(); if (h) try { h.send('{"f":' + me.id + ',"d":' + msg + '}'); } catch (e) {}
    }
  }
  webSocketClose(ws) { this.gone(ws); }
  webSocketError(ws) { this.gone(ws); }
  gone(ws) {
    const me = this.info(ws);
    if (me.role === 'host') for (const g of this.guests()) { try { g.send('{"ev":"hostgone"}'); g.close(4001, 'hostgone'); } catch (e) {} }
    else if (me.role === 'guest') { const h = this.host(); if (h) try { h.send(JSON.stringify({ ev: 'leave', id: me.id })); } catch (e) {} }
    try { ws.close(1000, 'bye'); } catch (e) {}
  }
}
export default {
  fetch(req, env) {
    const m = new URL(req.url).pathname.match(/^\/room\/([A-Z]{4})$/);
    if (!m) return new Response('Calico relay OK', { headers: { 'content-type': 'text/plain; charset=utf-8' } });
    return env.ROOMS.get(env.ROOMS.idFromName(m[1])).fetch(req);
  }
};

/* 동네 찜질방 3D · Supabase 연결부
   게임 코드는 claude.use('room'|'db'|'user') 형태로 멀티플레이/기록 기능을 불러요.
   이 파일이 그 자리를 Supabase Realtime + Postgres 로 채워 줘요. */
(function(){
  const C = window.JJIM_CONFIG || {};
  if (!window.supabase || !C.SUPABASE_URL || /YOUR-/.test(C.SUPABASE_URL + C.SUPABASE_ANON_KEY)) {
    console.warn('[jjim] Supabase 설정이 없어 혼자 하기 모드로 실행해요. config.js 를 채워 주세요.');
    return;
  }
  const sb = window.supabase.createClient(C.SUPABASE_URL, C.SUPABASE_ANON_KEY, { realtime: { params: { eventsPerSecond: 10 } } });
  const ROOM = C.ROOM || 'jjimjilbang-1';

  // 익명 로그인: 브라우저마다 고정된 사용자 ID (기록판 1인 1기록)
  let authP = null;
  function auth(){
    if (!authP) authP = (async () => {
      const { data: s } = await sb.auth.getSession();
      if (s && s.session) return s.session.user;
      const { data, error } = await sb.auth.signInAnonymously();
      if (error) { console.warn('[jjim] 익명 로그인 실패 — Supabase 에서 Anonymous sign-ins 를 켜 주세요.', error.message); return null; }
      return data.user;
    })();
    return authP;
  }

  /* ---------- room: 접속자 목록 + 상태 공유 ---------- */
  function makeRoom(){
    const myId = (crypto.randomUUID ? crypto.randomUUID() : String(Math.random()).slice(2)).slice(0, 12);
    const state = new Map();      // peer -> 마지막 상태
    const seen = new Map();       // peer -> 마지막 수신 시각
    let cb = null, mine = null, lastSent = 0, sendTimer = null, lastJson = '';
    const ch = sb.channel('jjim:' + ROOM, { config: { presence: { key: myId }, broadcast: { self: false } } });

    const emit = (left) => {
      if (!cb) return;
      const peers = [{ peer: myId, sameTab: true, kind: 'viewer', presence: mine || {} }];
      for (const [peer, presence] of state) peers.push({ peer, sameTab: false, kind: 'viewer', presence });
      try { cb({ peers, left: left || [] }); } catch (e) { console.error(e); }
    };
    const drop = (peer) => { if (state.has(peer)) { state.delete(peer); seen.delete(peer); emit([{ peer }]); } };

    ch.on('broadcast', { event: 's' }, ({ payload }) => {
      if (!payload || typeof payload.id !== 'string' || payload.id === myId) return;
      if (payload.st && typeof payload.st === 'object') { state.set(payload.id, payload.st); seen.set(payload.id, Date.now()); emit(); }
    });
    ch.on('broadcast', { event: 'hello' }, ({ payload }) => { if (payload && payload.id !== myId) flush(true); });
    ch.on('presence', { event: 'leave' }, ({ key }) => drop(key));
    ch.subscribe(async (status) => {
      if (status === 'SUBSCRIBED') {
        await ch.track({ at: Date.now() });
        ch.send({ type: 'broadcast', event: 'hello', payload: { id: myId } });
        flush(true);
      }
    });
    // 연결이 끊긴 채 사라진 사람 정리 (보통은 presence leave 로 바로 정리되고, 이건 안전장치)
    setInterval(() => { const t = Date.now(); for (const [p, s] of seen) if (t - s > 50000) drop(p); }, 5000);

    function flush(force){
      if (!mine) return;
      const j = JSON.stringify(mine);
      if (!force && j === lastJson) return;
      lastJson = j; lastSent = Date.now();
      ch.send({ type: 'broadcast', event: 's', payload: { id: myId, st: mine } });
    }
    // 무료 플랜 메시지 한도 아끼기
    // - 바뀐 게 있을 때만 보내요. 사람이 많을수록 덜 자주 (혼자~2명: 초당 3번 → 10명: 초당 1.5번 → 20명 이상: 초당 1번)
    // - 가만히 있으면 20초에 한 번만 '아직 있어요' 신호
    let tickAt = 0;
    setInterval(() => {
      const now = Date.now(), gap = Math.min(1000, Math.max(330, 250 + state.size * 40));
      if (now - tickAt < gap) return; tickAt = now;
      flush(now - lastSent > 20000);
    }, 100);

    return {
      onPeers(fn){ cb = fn; emit(); return () => { cb = null; }; },
      async presence(p){ mine = p; return true; }
    };
  }

  /* ---------- db: 기록판(records) ---------- */
  function makeDb(){
    return {
      mats: false, // 매트는 접속자끼리만 공유(나가면 사라짐)
      collection(name){
        return {
          onSnapshot(fn){
            if (name !== 'records') return () => {};
            const load = async () => {
              const { data, error } = await sb.from('records').select('id,n,hot,cold,at').order('hot', { ascending: false }).limit(200);
              if (error) return;
              fn({ docs: data.map(r => ({ id: r.id, data: () => ({ n: r.n, hot: r.hot, cold: r.cold, at: r.at }) })) });
            };
            load();
            const sub = sb.channel('jjim-records').on('postgres_changes', { event: '*', schema: 'public', table: 'records' }, load).subscribe();
            const iv = setInterval(load, 30000);
            return () => { clearInterval(iv); sb.removeChannel(sub); };
          },
          add(){ return Promise.reject(new Error('not supported')); }
        };
      },
      doc(path){
        const [col, id] = path.split('/');
        return {
          async set(v){
            if (col !== 'records') throw new Error('not supported');
            const row = { id, n: String(v.n || '손님').slice(0, 10), hot: Math.min(999, +v.hot || 0), cold: Math.min(999, +v.cold || 0), at: Date.now() };
            const { error } = await sb.from('records').upsert(row);
            if (error) throw error;
          },
          async delete(){ throw new Error('not supported'); }
        };
      }
    };
  }

  window.claude = {
    async use(cap){
      if (cap === 'room') return makeRoom();
      if (cap === 'db') return makeDb();
      if (cap === 'user') { const u = await auth(); return u ? { id: async () => u.id } : null; }
      return null;
    }
  };
})();

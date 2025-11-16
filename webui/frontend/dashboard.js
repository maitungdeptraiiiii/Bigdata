const sse = new EventSource('/metrics/stream');
const tbl = document.querySelector('#events_table tbody');

sse.onmessage = function(e) {
  try {
    const data = JSON.parse(e.data);
    // data expected to be {events_per_sec, active_players, purchases_5min, open_sessions, recent_events: [...]}
    document.getElementById('events_per_sec').innerText = data.events_per_sec ?? '-';
    document.getElementById('active_players').innerText = data.active_players ?? '-';
    document.getElementById('purchases_5min').innerText = data.purchases_5min ?? '-';
    document.getElementById('open_sessions').innerText = data.open_sessions ?? '-';

    if (Array.isArray(data.recent_events)) {
      // prepend events
      data.recent_events.forEach(ev => {
        const tr = document.createElement('tr');
        const timeTd = document.createElement('td'); timeTd.innerText = ev.timestamp || '-';
        const pidTd = document.createElement('td'); pidTd.innerText = ev.player_id || '-';
        const etTd = document.createElement('td'); etTd.innerText = ev.event_type || '-';
        const gTd = document.createElement('td'); gTd.innerText = ev.game_genre || '-';
        const mTd = document.createElement('td'); mTd.innerText = JSON.stringify(ev.metadata || {});
        tr.appendChild(timeTd); tr.appendChild(pidTd); tr.appendChild(etTd); tr.appendChild(gTd); tr.appendChild(mTd);
        tbl.insertBefore(tr, tbl.firstChild);
        // limit rows
        if (tbl.children.length > 50) tbl.removeChild(tbl.lastChild);
      });
    }
  } catch (err) {
    console.error('Invalid metrics payload', err);
  }
};

sse.onerror = function(err) {
  console.error('SSE error', err);
};

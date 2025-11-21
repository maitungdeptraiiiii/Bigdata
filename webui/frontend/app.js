async function fetchMetrics() {
  const res = await fetch("/api/live_metrics");
  const data = await res.json();
  document.getElementById("metrics").innerHTML = `
    <b>Window:</b> ${data.window_start} → ${data.window_end}<br>
    <b>Events:</b> ${data.event_count}
  `;
}

async function fetchEvents() {
  const res = await fetch("/api/recent_events");
  const events = await res.json();
  const div = document.getElementById("events");

  div.innerHTML = "";

  events.forEach(e => {
    const item = document.createElement("div");
    item.className = "event-item";
    item.innerHTML = `
      <b>${e.event_type}</b> — Player ${e.player_id}<br>
      <small>${e.timestamp}</small>
    `;
    div.appendChild(item);
  });
}

async function fetchPlayers() {
  const res = await fetch("/api/players");
  const ids = await res.json();
  const sel = document.getElementById("player-select");

  ids.forEach(id => {
    const opt = document.createElement("option");
    opt.value = id;
    opt.innerHTML = "Player " + id;
    sel.appendChild(opt);
  });
}

async function fetchPlayerDetails() {
  const pid = document.getElementById("player-select").value;
  if (!pid) return;

  const res = await fetch(`/api/player/${pid}`);
  const data = await res.json();

  document.getElementById("player-details").innerHTML =
    JSON.stringify(data, null, 2);
}

document.getElementById("player-select").addEventListener("change", fetchPlayerDetails);

fetchPlayers();

// realtime polling
setInterval(fetchMetrics, 1500);
setInterval(fetchEvents, 2000);
setInterval(fetchPlayerDetails, 3000);

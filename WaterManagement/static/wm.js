/* WaterManagement - utilidades comunes de las tres interfaces (sin librerías externas) */
const $ = (id) => document.getElementById(id);

function esc(s) {
    return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function hora(ts) {
    const d = ts ? new Date(ts * 1000) : new Date();
    return d.toLocaleTimeString("es-ES", { hour12: false });
}

async function api(url, cuerpo) {
    const opciones = cuerpo === undefined ? { cache: "no-store" } : {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(cuerpo),
    };
    try {
        const r = await fetch(url, opciones);
        const datos = await r.json().catch(() => ({}));
        datos._status = r.status;
        return datos;
    } catch (e) {
        return { ok: false, error: "No se puede contactar con el servidor", _status: 0 };
    }
}

/* Dibuja líneas nuevas en una consola. filtro(linea) decide si se muestra. */
function pintarConsola(contenedor, lineas, filtro, mostrarOrigen) {
    if (!lineas.length) return;
    const abajo = contenedor.scrollTop + contenedor.clientHeight >= contenedor.scrollHeight - 30;
    for (const l of lineas) {
        if (filtro && !filtro(l)) continue;
        const d = document.createElement("div");
        d.className = l.cls || "";
        d.dataset.n = l.n;
        d.dataset.src = l.src;
        d.innerHTML = '<span class="t">' + hora(l.t) + "</span>" +
            (mostrarOrigen ? '<span class="src">' + esc(l.src) + "</span>" : "") + esc(l.txt);
        contenedor.appendChild(d);
    }
    while (contenedor.children.length > 600) contenedor.removeChild(contenedor.firstChild);
    if (abajo) contenedor.scrollTop = contenedor.scrollHeight;
}

/* Gráfica de línea sencilla (SVG) para una serie de números. */
function dibujarSerie(svg, datos, maximo, color) {
    const W = 600, H = 120, m = 6;
    maximo = maximo || Math.max(20, ...datos);
    let html = "";
    for (let i = 0; i <= 3; i++) {
        const y = m + (H - 2 * m) * i / 3;
        html += '<line x1="0" x2="' + W + '" y1="' + y + '" y2="' + y + '" stroke="#24344f" stroke-width="1" />';
    }
    if (datos.length > 1) {
        const paso = W / Math.max(datos.length - 1, 1);
        const puntos = datos.map((v, i) => (i * paso).toFixed(1) + "," + (H - m - (H - 2 * m) * Math.min(v / maximo, 1)).toFixed(1));
        html += '<polygon points="0,' + H + " " + puntos.join(" ") + " " + ((datos.length - 1) * paso).toFixed(1) + "," + H +
            '" fill="' + color + '" fill-opacity=".14" />';
        html += '<polyline points="' + puntos.join(" ") + '" fill="none" stroke="' + color + '" stroke-width="2.4" stroke-linejoin="round" />';
    }
    svg.setAttribute("viewBox", "0 0 " + W + " " + H);
    svg.innerHTML = html;
}

/* Cronómetro: segundos transcurridos desde un instante (en segundos de reloj del servidor). */
function transcurrido(desde, ahoraServidor) {
    return desde ? Math.max(0, ahoraServidor - desde) : 0;
}

/* WaterManagement - estilos comunes de las tres interfaces (Central, Operario y Estación) */
:root {
    --fondo: #0b1220; --panel: #121c2e; --panel2: #18253b; --borde: #24344f;
    --texto: #e8eef7; --suave: #8ea0ba; --acento: #38bdf8;
    --disp: #22a559; --reg: #2ee57f; --fuga: #e5484d; --fuera: #f08a24; --desc: #6b7a90;
    --azul: #5aa9ff; --amarillo: #f5c542;
}
* { box-sizing: border-box; }
html, body { margin: 0; }
html { background: #0b1220; }
body {
    font-family: "Segoe UI", system-ui, -apple-system, Roboto, Arial, sans-serif;
    background: radial-gradient(1200px 600px at 10% -10%, #14305a 0%, var(--fondo) 55%) fixed;
    color: var(--texto); min-height: 100vh; padding: 22px 26px 40px;
}
h1, h2, h3 { margin: 0; font-weight: 700; }
.cabecera { display: flex; align-items: center; gap: 14px; flex-wrap: wrap; margin-bottom: 20px; }
.logo { width: 46px; height: 46px; border-radius: 13px; display: grid; place-items: center; font-size: 25px;
    background: linear-gradient(135deg, #38bdf8, #22a559); box-shadow: 0 6px 20px rgba(56,189,248,.35); }
.titulo h1 { font-size: 22px; letter-spacing: .01em; }
.titulo small { color: var(--suave); font-size: 13px; }
.espacio { flex: 1; }
.chip { display: inline-flex; align-items: center; gap: 8px; background: #0f1a2c; border: 1px solid var(--borde);
    border-radius: 999px; padding: 6px 13px; font-size: 13px; color: var(--suave); white-space: nowrap; }
.chip b { color: var(--texto); font-weight: 600; }
.punto { width: 9px; height: 9px; border-radius: 50%; background: var(--desc); display: inline-block; }
.punto.ok { background: var(--reg); box-shadow: 0 0 8px rgba(46,229,127,.8); }
.punto.mal { background: var(--fuga); box-shadow: 0 0 8px rgba(229,72,77,.8); }
.punto.aviso { background: var(--amarillo); }

.rejilla { display: grid; gap: 18px; }
.dos { grid-template-columns: minmax(300px, 380px) 1fr; align-items: start; }
.tarjeta { background: linear-gradient(180deg, var(--panel), #0f1a2c); border: 1px solid var(--borde);
    border-radius: 16px; padding: 18px 20px; box-shadow: 0 10px 30px rgba(0,0,0,.25); }
.tarjeta + .tarjeta { margin-top: 18px; }
.tarjeta h2 { font-size: 13px; text-transform: uppercase; letter-spacing: .09em; color: var(--suave); margin-bottom: 14px;
    display: flex; align-items: center; gap: 8px; }
.tarjeta h2 .espacio { flex: 1; }
.suave { color: var(--suave); }
.pequeno { font-size: 12.5px; }

label { display: block; font-size: 12.5px; color: var(--suave); margin: 12px 0 5px; }
input[type=text], input[type=number], textarea, select {
    width: 100%; background: #0b1424; color: var(--texto); border: 1px solid var(--borde); border-radius: 10px;
    padding: 10px 12px; font-size: 15px; font-family: inherit; outline: none; }
input:focus, textarea:focus, select:focus { border-color: var(--acento); box-shadow: 0 0 0 3px rgba(56,189,248,.18); }
textarea { font-family: Consolas, "Courier New", monospace; font-size: 13.5px; min-height: 110px; resize: vertical; }
input[type=range] { width: 100%; accent-color: var(--acento); }
.fila { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
.fila > * { flex: 1; }
.fila > .fijo { flex: none; }

button, .boton {
    font-family: inherit; font-size: 14px; font-weight: 600; cursor: pointer; border-radius: 10px; padding: 10px 16px;
    border: 1px solid var(--borde); background: var(--panel2); color: var(--texto); transition: transform .06s, filter .15s, background .15s; }
button:hover:not(:disabled) { filter: brightness(1.18); }
button:active:not(:disabled) { transform: translateY(1px); }
button:disabled { opacity: .4; cursor: not-allowed; }
button.principal { background: linear-gradient(135deg, #1b7fd1, #22a559); border-color: transparent; color: #fff; }
button.peligro { background: #5b1d22; border-color: #8f2c33; color: #ffd9db; }
button.aviso { background: #5b3a12; border-color: #8f5f20; color: #ffe2b8; }
button.ancho { width: 100%; }
button.pequeno { padding: 5px 11px; font-size: 12.5px; }

.insignia { display: inline-block; padding: 6px 14px; border-radius: 999px; font-weight: 700; font-size: 13px; letter-spacing: .03em; color: #fff; }
.st-AVAILABLE { background: var(--disp); }
.st-WATERING { background: var(--reg); color: #04210f; animation: parpadeo 1s infinite; }
.st-KO { background: var(--fuga); }
.st-OUT_OF_SERVICE { background: var(--fuera); color: #2b1600; }
.st-DISCONNECTED { background: var(--desc); }
@keyframes parpadeo { 0%, 100% { opacity: 1; box-shadow: 0 0 14px rgba(46,229,127,.7); } 50% { opacity: .38; box-shadow: none; } }

/* consola de mensajes */
.consola { background: #070d18; border: 1px solid var(--borde); border-radius: 12px; height: 270px; overflow-y: auto;
    padding: 10px 12px; font-family: Consolas, "Courier New", monospace; font-size: 12.8px; line-height: 1.5; }
.consola div { white-space: pre-wrap; word-break: break-word; }
.consola .t { color: #50627f; margin-right: 8px; }
.consola .rojo { color: #ff7b80; } .consola .verde { color: #4be28a; } .consola .azul { color: var(--azul); }
.consola .amarillo { color: var(--amarillo); } .consola .cian { color: #5fd7e8; }
.consola .entrada { color: #c6a7ff; } .consola .sistema { color: #66789a; font-style: italic; }
.consola .src { display: inline-block; min-width: 62px; color: #6f83a6; }

table { width: 100%; border-collapse: collapse; font-size: 13.5px; }
th { text-align: left; color: var(--suave); font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: .06em; padding: 6px 10px; }
td { padding: 9px 10px; border-top: 1px solid var(--borde); font-variant-numeric: tabular-nums; }
.vacio { color: var(--suave); font-size: 13.5px; padding: 8px 0; }

.avisos { background: #2a1519; border: 1px solid #6f2a31; border-left: 4px solid var(--fuga); color: #ffd3d6;
    padding: 10px 14px; border-radius: 10px; margin-bottom: 16px; font-size: 14px; display: none; }
.avisos.visible { display: block; }
.leyenda { display: flex; gap: 14px; flex-wrap: wrap; font-size: 12.5px; color: var(--suave); }
.leyenda span { display: inline-flex; align-items: center; gap: 6px; }
.cuadrito { width: 12px; height: 12px; border-radius: 3px; display: inline-block; }

@media (max-width: 900px) { .dos { grid-template-columns: 1fr; } body { padding: 16px; } }

.tarjeta h2 select { width: auto; padding: 4px 8px; font-size: 12px; text-transform: none; letter-spacing: 0; }

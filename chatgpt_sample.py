from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.encoders import jsonable_encoder
import pandas as pd
import io
import re

from rosreestr2coord import Area  # библиотека для получения GeoJSON по КН

app = FastAPI()

INDEX_HTML = """
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <title>Кадастровые участки</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <style>
    * { box-sizing: border-box; }
    body { margin:0; padding:0; font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif; }
    #layout { display:flex; height:100vh; }
    #sidebar {
      width:340px;
      max-width:40%;
      border-right:1px solid #ddd;
      padding:12px;
      overflow-y:auto;
    }
    #map { flex:1; }

    #fileInput { margin-bottom:8px; width:100%; }
    #status { font-size:12px; color:#555; margin-bottom:8px; }

    .lot-item {
      padding:6px 8px;
      border-radius:4px;
      margin-bottom:4px;
      cursor:pointer;
      border:1px solid transparent;
      font-size:13px;
    }
    .lot-item:hover {
      background:#f5f5f5;
      border-color:#ccc;
    }
    .lot-item.active {
      background:#e0f2ff;
      border-color:#2196f3;
    }
    .lot-title { font-weight:600; font-size:14px; }
    .lot-sub { font-size:12px; color:#666; }
  </style>
</head>
<body>
<div id="layout">
  <div id="sidebar">
    <input type="file" id="fileInput" accept=".xlsx,.xls,.csv" />
    <div id="status">Загрузите выгрузку из реестра лотов (.xlsx)</div>
    <div id="lots"></div>
  </div>
  <div id="map"></div>
</div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
  const map = L.map('map').setView([55.79, 49.12], 8);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap'
  }).addTo(map);

  let layer;
  const lotsContainer = document.getElementById('lots');
  const statusEl = document.getElementById('status');

  function showDetails(props) {
    statusEl.innerHTML =
      '<b>' + (props.cadastral_number || '') + '</b><br>' +
      (props.address || '') + '<br>' +
      'Начальная цена: ' + (props.price_start ?? '-') + '<br>' +
      'Статус лота: ' + (props.status || '-') + '<br>' +
      (props.notice_url ? '<a href="' + props.notice_url + '" target="_blank">Ссылка на лот</a>' : '');
  }

  function renderList(fc) {
    lotsContainer.innerHTML = '';
    fc.features.forEach((f, idx) => {
      const div = document.createElement('div');
      div.className = 'lot-item';
      div.dataset.index = idx;
      const p = f.properties;
      div.innerHTML =
        '<div class="lot-title">' + (p.cadastral_number || 'без номера') + '</div>' +
        '<div class="lot-sub">' + (p.address || '') + '</div>';
      div.onclick = () => {
        const g = L.geoJSON(f);
        map.fitBounds(g.getBounds(), { maxZoom: 16 });
        showDetails(p);
        document.querySelectorAll('.lot-item').forEach(el => el.classList.remove('active'));
        div.classList.add('active');
      };
      lotsContainer.appendChild(div);
    });
  }

  function drawOnMap(fc) {
    if (layer) {
      map.removeLayer(layer);
    }
    layer = L.geoJSON(fc, {
      onEachFeature: (feature, lyr) => {
        lyr.on('click', () => {
          showDetails(feature.properties);
        });
      },
      style: {
        weight: 1,
        color: '#ff0000',
        fillOpacity: 0.2
      }
    }).addTo(map);

    const bounds = layer.getBounds();
    if (bounds.isValid()) {
      map.fitBounds(bounds);
    }
  }

  document.getElementById('fileInput').addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    statusEl.textContent = 'Загружаю и обрабатываю файл...';

    const formData = new FormData();
    formData.append('file', file);

    try {
      const res = await fetch('/api/upload', { method: 'POST', body: formData });
      if (!res.ok) {
        throw new Error('HTTP ' + res.status);
      }
      const fc = await res.json();
      if (!fc.features || fc.features.length === 0) {
        statusEl.textContent = 'Не удалось получить ни одного участка. Проверь формат файла.';
        return;
      }
      drawOnMap(fc);
      renderList(fc);
      statusEl.textContent = 'Найдено участков: ' + fc.features.length;
    } catch (err) {
      console.error(err);
      statusEl.textContent = 'Ошибка при загрузке или парсинге файла.';
    }
  });
</script>
</body>
</html>
"""

# --------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ БЭКЕНДА ---------

CADASTRAL_RE = re.compile(r"\d+:\d+:\d+:\d+")


def extract_cadastral_numbers(text: str):
    if not text:
        return []
    return CADASTRAL_RE.findall(str(text))


def load_lots_from_excel(content: bytes):
    """
    Ожидается именно такой формат, как в выгрузке:
    первая строка — названия колонок, начиная с 'Вид торгов' и т.д.
    """
    # читаем без заголовка, потом первую строку превращаем в header
    df_raw = pd.read_excel(io.BytesIO(content), sheet_name=0, header=None)
    header = df_raw.iloc[0]
    data = df_raw[1:].copy()
    data.columns = header
    data = data.reset_index(drop=True)
    return data


def row_to_properties(row, cadastral_number: str):
    def clean(v):
        if pd.isna(v):
            return None
        return v

    return {
        "cadastral_number": cadastral_number,
        "lot_number": clean(row.get("Номер лота")),
        "subject": clean(row.get("Субъект РФ")),
        "address": clean(row.get("Местонахождение имущества")),
        "status": clean(row.get("Статус лота")),
        "price_start": clean(row.get("Начальная цена")),
        "price_final": clean(row.get("Итоговая цена")),
        "notice_url": clean(row.get("Ссылка на лот в ОЧ Реестра лотов")),
    }


# --------- ЭНДПОИНТЫ ---------


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML


@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    content = await file.read()

    # Excel-выгрузка из реестра
    lots_df = load_lots_from_excel(content)

    features = []

    for _, row in lots_df.iterrows():
        desc = row.get("Характеристики имущества", "")
        cadastral_numbers = extract_cadastral_numbers(desc)

        if not cadastral_numbers:
            continue

        # На каждый лот можно брать первый КН (если их несколько, можно расширить логику)
        kn = cadastral_numbers[0]

        try:
            area = Area(kn)  # обращение к НСПД / Росреестру под капотом
            gj = area.to_geojson()
        except Exception:
            # если по участку ничего не нашли / ошибка — просто пропускаем
            continue

        # rosreestr2coord может отдавать Feature или FeatureCollection
        geometry = None
        if gj.get("type") == "FeatureCollection":
            feats = gj.get("features") or []
            if not feats:
                continue
            geometry = feats[0].get("geometry")
        elif gj.get("type") == "Feature":
            geometry = gj.get("geometry")
        else:
            continue

        if not geometry:
            continue

        props = row_to_properties(row, kn)
        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": props,
        }
        features.append(feature)

    feature_collection = {
        "type": "FeatureCollection",
        "features": features,
    }

    return jsonable_encoder(feature_collection)

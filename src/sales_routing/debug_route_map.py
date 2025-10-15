import folium
import json
import psycopg2
from statistics import mean

# =============================
# 🔧 Parâmetros de configuração
# =============================
tenant_id = 1
cluster_id = 66
subcluster_seq = 2

# =============================
# 🔌 Conexão com o banco
# =============================
conn = psycopg2.connect(
    dbname="sales_routing_db",
    user="postgres",
    password="postgres",
    host="sales_router_db",
    port="5432"
)
cur = conn.cursor()

# -------------------------------------------------
# 1️⃣ Busca coordenadas da rota do subcluster
# -------------------------------------------------
cur.execute("""
    SELECT rota_coord
    FROM sales_subcluster
    WHERE tenant_id = %s
      AND cluster_id = %s
      AND subcluster_seq = %s;
""", (tenant_id, cluster_id, subcluster_seq))
row = cur.fetchone()
rota_coord = json.loads(row[0]) if row and isinstance(row[0], str) else (row[0] if row else [])

# -------------------------------------------------
# 2️⃣ Busca os PDVs associados a este subcluster
# -------------------------------------------------
cur.execute("""
    SELECT p.pdv_lat, p.pdv_lon, p.cnpj, p.cidade, p.uf
    FROM sales_subcluster_pdv sc
    JOIN pdvs p ON sc.pdv_id = p.id
    WHERE sc.tenant_id = %s
      AND sc.cluster_id = %s
      AND sc.subcluster_seq = %s;
""", (tenant_id, cluster_id, subcluster_seq))
pdvs = cur.fetchall()

# -------------------------------------------------
# 3️⃣ Busca o centro do cluster
# -------------------------------------------------
cur.execute("""
    SELECT centro_lat, centro_lon
    FROM cluster_setor
    WHERE id = %s AND tenant_id = %s;
""", (cluster_id, tenant_id))
centro = cur.fetchone()
cur.close()
conn.close()

# -------------------------------------------------
# 4️⃣ Criação do mapa base
# -------------------------------------------------
if pdvs:
    lat_centro = mean([p[0] for p in pdvs if p[0]])
    lon_centro = mean([p[1] for p in pdvs if p[1]])
else:
    lat_centro, lon_centro = -22.47, -44.45  # fallback Resende

m = folium.Map(location=[lat_centro, lon_centro], zoom_start=13)

# -------------------------------------------------
# 5️⃣ Desenha a rota
# -------------------------------------------------
if rota_coord:
    folium.PolyLine(
        [(c["lat"], c["lon"]) for c in rota_coord],
        color="blue", weight=3, opacity=0.8, tooltip="Rota principal"
    ).add_to(m)
    folium.Marker(
        [rota_coord[0]["lat"], rota_coord[0]["lon"]],
        icon=folium.Icon(color="green"), tooltip="Início da rota"
    ).add_to(m)
    folium.Marker(
        [rota_coord[-1]["lat"], rota_coord[-1]["lon"]],
        icon=folium.Icon(color="darkred"), tooltip="Fim da rota"
    ).add_to(m)

# -------------------------------------------------
# 6️⃣ Adiciona os PDVs (marcadores azuis claros)
# -------------------------------------------------
for lat, lon, cnpj, cidade, uf in pdvs:
    if lat and lon:
        folium.CircleMarker(
            location=[lat, lon],
            radius=4,
            color="blue",
            fill=True,
            fill_opacity=0.7,
            tooltip=f"{cnpj} - {cidade}/{uf}"
        ).add_to(m)

# -------------------------------------------------
# 7️⃣ Adiciona o centro do cluster
# -------------------------------------------------
if centro and centro[0] and centro[1]:
    folium.Marker(
        [centro[0], centro[1]],
        icon=folium.Icon(color="red", icon="info-sign"),
        tooltip="Centro do Cluster"
    ).add_to(m)

# -------------------------------------------------
# 8️⃣ Salva o mapa
# -------------------------------------------------
out_path = f"/tmp/rota_cluster{cluster_id}_sub{subcluster_seq}.html"
m.save(out_path)
print(f"✅ Mapa salvo em {out_path}")

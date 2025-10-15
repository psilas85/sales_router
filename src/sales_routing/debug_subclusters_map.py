import pandas as pd
import folium
from folium.plugins import MarkerCluster

# Caminho do CSV exportado do banco
csv_path = "/tmp/subclusters_1_3_resende.csv"
output_path = "/tmp/subclusters_1_3_resende_map.html"

# Carrega os dados
df = pd.read_csv(csv_path, sep=";")

# Define cores por subcluster
cores = {1: "red", 3: "orange"}

# Cria mapa centralizado no centro aproximado de Resende/RJ
m = folium.Map(location=[-22.468, -44.446], zoom_start=9)

# Adiciona marcadores agrupados
marker_cluster = MarkerCluster().add_to(m)

for _, row in df.iterrows():
    cor = cores.get(row["subcluster_seq"], "gray")
    folium.CircleMarker(
        location=[row["pdv_lat"], row["pdv_lon"]],
        radius=7,
        color=cor,
        fill=True,
        fill_opacity=0.9,
        popup=(
            f"Subcluster: {row['subcluster_seq']}<br>"
            f"PDV ID: {row['pdv_id']}<br>"
            f"CNPJ: {row['cnpj']}<br>"
            f"Cidade: {row['cidade']} - {row['uf']}<br>"
            f"Lat/Lon: ({row['pdv_lat']:.6f}, {row['pdv_lon']:.6f})"
        ),
    ).add_to(marker_cluster)

# Adiciona o centro de Resende (manual ou obtido do cluster principal)
folium.Marker(
    location=[-22.468, -44.446],
    icon=folium.Icon(color="blue", icon="home"),
    popup="Centro de Resende (Cluster 66)"
).add_to(m)

# Salva mapa
m.save(output_path)
print(f"✅ Mapa salvo em: {output_path}")

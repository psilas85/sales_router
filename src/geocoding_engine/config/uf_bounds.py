#sales_router/src/geocoding_engine/config/uf_bounds.py

# ============================================================
# 📍 Limites geográficos por UF (lat/lon)
# Gerado da malha municipal do IBGE: bounding box real por UF + folga de 0.15°.
# É apenas um pré-filtro grosso — a validação precisa é o polígono do município.
# ============================================================

UF_BOUNDS = {
    "AC": {"lat_min": -11.30, "lat_max": -6.96, "lon_min": -74.14, "lon_max": -66.47},
    "AL": {"lat_min": -10.65, "lat_max": -8.66, "lon_min": -38.39, "lon_max": -35.00},
    "AM": {"lat_min": -9.97, "lat_max": 2.39, "lon_min": -73.95, "lon_max": -55.94},
    "AP": {"lat_min": -1.39, "lat_max": 4.65, "lon_min": -55.03, "lon_max": -49.73},
    "BA": {"lat_min": -18.50, "lat_max": -8.38, "lon_min": -46.73, "lon_max": -37.19},
    "CE": {"lat_min": -8.01, "lat_max": -2.63, "lon_min": -41.57, "lon_max": -37.10},
    "DF": {"lat_min": -16.20, "lat_max": -15.35, "lon_min": -48.44, "lon_max": -47.16},
    "ES": {"lat_min": -21.45, "lat_max": -17.74, "lon_min": -42.03, "lon_max": -28.70},
    "GO": {"lat_min": -19.65, "lat_max": -12.24, "lon_min": -53.40, "lon_max": -45.76},
    "MA": {"lat_min": -10.41, "lat_max": -0.90, "lon_min": -48.91, "lon_max": -41.65},
    "MG": {"lat_min": -23.07, "lat_max": -14.08, "lon_min": -51.20, "lon_max": -39.71},
    "MS": {"lat_min": -24.22, "lat_max": -17.02, "lon_min": -58.32, "lon_max": -50.77},
    "MT": {"lat_min": -18.19, "lat_max": -7.20, "lon_min": -61.78, "lon_max": -50.07},
    "PA": {"lat_min": -9.99, "lat_max": 2.78, "lon_min": -59.05, "lon_max": -45.91},
    "PB": {"lat_min": -8.45, "lat_max": -5.88, "lon_min": -38.92, "lon_max": -34.64},
    "PE": {"lat_min": -9.63, "lat_max": -3.65, "lon_min": -41.51, "lon_max": -32.23},
    "PI": {"lat_min": -11.08, "lat_max": -2.60, "lon_min": -46.18, "lon_max": -40.22},
    "PR": {"lat_min": -26.87, "lat_max": -22.37, "lon_min": -54.77, "lon_max": -47.87},
    "RJ": {"lat_min": -23.52, "lat_max": -20.61, "lon_min": -45.04, "lon_max": -40.81},
    "RN": {"lat_min": -7.13, "lat_max": -4.67, "lon_min": -38.73, "lon_max": -34.82},
    "RO": {"lat_min": -13.84, "lat_max": -7.83, "lon_min": -66.96, "lon_max": -59.62},
    "RR": {"lat_min": -1.73, "lat_max": 5.42, "lon_min": -64.98, "lon_max": -58.74},
    "RS": {"lat_min": -33.90, "lat_max": -26.93, "lon_min": -57.80, "lon_max": -49.54},
    "SC": {"lat_min": -29.51, "lat_max": -25.81, "lon_min": -53.99, "lon_max": -48.18},
    "SE": {"lat_min": -11.72, "lat_max": -9.37, "lon_min": -38.40, "lon_max": -36.25},
    "SP": {"lat_min": -25.51, "lat_max": -19.63, "lon_min": -53.26, "lon_max": -44.01},
    "TO": {"lat_min": -13.62, "lat_max": -5.02, "lon_min": -50.89, "lon_max": -45.55},
}

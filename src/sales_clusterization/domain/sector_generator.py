#sales_clusterization/domain/sector_generator.py

from typing import List, Tuple
import numpy as np
from sklearn.cluster import KMeans, DBSCAN
from .entities import PDV, Setor
from .k_estimator import _haversine_km

def _raios_cluster(centro:Tuple[float,float], pts:List[Tuple[float,float]]):
    if not pts: return 0.0, 0.0
    dists = [_haversine_km(centro, p) for p in pts]
    dists.sort()
    med = dists[len(dists)//2]
    p95 = dists[int(0.95*len(dists))-1] if len(dists)>=2 else dists[-1]
    return med, p95

def kmeans_setores(pdvs: List[PDV], k:int, random_state:int=42):
    X = np.array([[p.lat, p.lon] for p in pdvs])
    km = KMeans(n_clusters=k, random_state=random_state, n_init="auto").fit(X)
    labels = km.labels_
    centers = km.cluster_centers_
    setores: List[Setor] = []
    for cid in range(k):
        idx = np.where(labels==cid)[0]
        pts = [ (pdvs[i].lat, pdvs[i].lon) for i in idx ]
        c = tuple(centers[cid])
        med,p95 = _raios_cluster(c, pts)
        setores.append(Setor(cluster_label=cid, centro_lat=c[0], centro_lon=c[1],
                             n_pdvs=len(idx), raio_med_km=med, raio_p95_km=p95))
    return setores, labels

def dbscan_setores(pdvs: List[PDV], eps_km:float=1.0, min_samples:int=20):
    eps_deg = eps_km/111.0
    X = np.array([[p.lat, p.lon] for p in pdvs])
    db = DBSCAN(eps=eps_deg, min_samples=min_samples).fit(X)
    labels = db.labels_
    setores: List[Setor] = []
    for cid in sorted(set(labels)):
        if cid == -1:
            continue
        idx = np.where(labels==cid)[0]
        pts = [ (pdvs[i].lat, pdvs[i].lon) for i in idx ]
        c = (float(np.mean([p[0] for p in pts])), float(np.mean([p[1] for p in pts])))
        med,p95 = _raios_cluster(c, pts)
        setores.append(Setor(cluster_label=cid, centro_lat=c[0], centro_lon=c[1],
                             n_pdvs=len(idx), raio_med_km=med, raio_p95_km=p95))
    return setores, labels

import pandas as pd
from pathlib import Path

# ===== CONFIG =====
ARQUIVO_CSV = "arquivo.csv"      # caminho do seu csv
ARQUIVO_XLSX = "arquivo.xlsx"    # saída
DELIMITADOR = ","                # no seu exemplo é vírgula
ENCODING = "utf-8"               # mude para latin-1 se der erro
# ==================

def main():
    print("Lendo CSV...")

    df = pd.read_csv(
        ARQUIVO_CSV,
        sep=DELIMITADOR,
        dtype=str,               # NÃO inferir tipo
        keep_default_na=False,   # vazio fica ""
        engine="python"          # mais tolerante a CSV ruim
    )

    print(f"Linhas: {len(df)} | Colunas: {len(df.columns)}")

    print("Salvando XLSX...")
    df.to_excel(ARQUIVO_XLSX, index=False)

    print("Concluído com sucesso.")

if __name__ == "__main__":
    main()

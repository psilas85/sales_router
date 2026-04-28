import argparse
import re

from geocoding_engine.domain.cache_key_builder import (
    build_cache_key,
    build_canonical_address,
)
from geocoding_engine.infrastructure.database_reader import DatabaseReader


ADDRESS_PATTERN = re.compile(r"^(?P<street>.+?)\s+(?P<number>[^,]+),\s*(?P<city>.+?)\s*-\s*(?P<uf>[A-Za-z]{2})$")


def parse_cached_address(endereco: str):
    if not endereco:
        return None

    match = ADDRESS_PATTERN.match(str(endereco).strip())
    if not match:
        return None

    return {
        "logradouro": match.group("street").strip(),
        "numero": match.group("number").strip(),
        "cidade": match.group("city").strip(),
        "uf": match.group("uf").strip(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    reader = DatabaseReader()
    conn = reader.conn

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, endereco, endereco_normalizado, origem
            FROM enderecos_cache
            ORDER BY id
            """
        )
        rows = cur.fetchall()

    if args.limit > 0:
        rows = rows[:args.limit]

    by_endereco = {
        str(row[1]).strip(): row[0]
        for row in rows
        if row[1]
    }
    by_normalizado = {
        str(row[2]).strip(): row[0]
        for row in rows
        if row[2]
    }

    total = len(rows)
    parse_fail = 0
    unchanged = 0
    needs_update = 0
    updated = 0
    conflict_address = 0
    conflict_key = 0

    for row in rows:
        cache_id, endereco, endereco_normalizado, _origem = row
        parsed = parse_cached_address(endereco)

        if not parsed:
            parse_fail += 1
            continue

        canonical_address = build_canonical_address(
            parsed["logradouro"],
            parsed["numero"],
            parsed["cidade"],
            parsed["uf"],
        )
        canonical_key = build_cache_key(
            parsed["logradouro"],
            parsed["numero"],
            parsed["cidade"],
            parsed["uf"],
        )

        if endereco == canonical_address and endereco_normalizado == canonical_key:
            unchanged += 1
            continue

        needs_update += 1

        existing_address_id = by_endereco.get(canonical_address)
        if existing_address_id is not None and existing_address_id != cache_id:
            conflict_address += 1
            continue

        existing_key_id = by_normalizado.get(canonical_key)
        if existing_key_id is not None and existing_key_id != cache_id:
            conflict_key += 1
            continue

        if args.apply:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE enderecos_cache
                    SET endereco = %s,
                        endereco_normalizado = %s,
                        atualizado_em = NOW()
                    WHERE id = %s
                    """,
                    (canonical_address, canonical_key, cache_id),
                )
            conn.commit()
            updated += 1

            by_endereco.pop(str(endereco).strip(), None)
            if endereco_normalizado:
                by_normalizado.pop(str(endereco_normalizado).strip(), None)
            by_endereco[canonical_address] = cache_id
            by_normalizado[canonical_key] = cache_id

    print(f"total={total}")
    print(f"parse_fail={parse_fail}")
    print(f"unchanged={unchanged}")
    print(f"needs_update={needs_update}")
    print(f"conflict_address={conflict_address}")
    print(f"conflict_key={conflict_key}")
    print(f"updated={updated}")
    print(f"mode={'apply' if args.apply else 'dry-run'}")


if __name__ == "__main__":
    main()
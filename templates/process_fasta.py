import argparse
import json
import os

import psycopg2
import psycopg2.extras

def read_combined_tsv(path):
    """
    返回 list of dict: {source_db, sacc, name, score, identity, e_value}
    期望文件以 tab 分隔，第一行为 header
    """
    hits = []
    if not os.path.exists(path):
        return hits
    with open(path, "r", encoding="utf-8") as fh:
        first = True
        for ln in fh:
            ln = ln.rstrip("\n")
            if not ln:
                continue
            if first:
                # skip header line if it matches header
                first = False
                if ln.lower().startswith("source_db"):
                    continue
            parts = ln.split("\t")
            # we expect at least 6 cols: source_db, sacc, stitle, bitscore, pident, evalue
            if len(parts) < 6:
                # skip malformed line
                continue
            source_db = parts[0]
            sacc = parts[1]
            stitle = parts[2]
            try:
                bitscore = float(parts[3]) if parts[3] != "" else 0.0
            except Exception:
                bitscore = 0.0
            try:
                pident = float(parts[4]) if parts[4] != "" else None
            except Exception:
                pident = None
            try:
                evalue = float(parts[5]) if parts[5] != "" else None
            except Exception:
                evalue = None
            hits.append({
                "source_db": source_db,
                "sacc": sacc,
                "name": stitle,
                "score": bitscore,
                "identity": pident,
                "e_value": evalue
            })
    return hits

def fetch_hit_metadata(conn, source_db, sacc):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql = f"SELECT * FROM {source_db} WHERE accession = %s LIMIT 1"
        try:
            cur.execute(sql, (sacc,))
            row = cur.fetchone()
        except Exception:
            # if column doesn't exist or other DB error, try next
            row = None
        if row:
            organism = row["organism"]
            external_url = row["external_url"]
            attributes = row["attributes"]
            return {"organism": organism, "external_url": external_url, "attributes": attributes}
    return None

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="combined tsv path")
    p.add_argument("--task", required=True)
    args = p.parse_args()

    combined_path = args.input
    task_id = args.task
    conn = psycopg2.connect(
        user="postgres",
        password="0909",
        database="venusDB_API",
        host="localhost",
        port="5432"
    )

    hits = read_combined_tsv(combined_path)
    if not hits:
        print("No hits found; wrote empty results.")
        return

    try:
        # 1) load distinct source_db list and fetch their source_type from central table
        source_list = sorted({h["source_db"] for h in hits})
        source_info = {}
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                for src in source_list:
                    cur.execute("SELECT source_type FROM databases WHERE id = %s", (src,))
                    row = cur.fetchone()
                    if row:
                        source_info[src] = row["source_type"]
                    else:
                        # unknown source_db: mark as internal unknown but continue
                        source_info[src] = {"id": src, "label": src, "source_type": "external"}

        # 2) for each hit, fetch metadata from the source table (if available)
        normalized_hits = []
        for h in hits:
            src = h["source_db"]
            sacc = h["sacc"]
            name = h["name"]
            score = h["score"]
            identity = h["identity"]
            evalue = h["e_value"]

            # default values
            source_type = source_info.get(src)

            # try fetch hit-level metadata from the source table
            hit_meta = fetch_hit_metadata(conn, src, sacc)  # may return None
            organism = None
            external_url = None
            attributes = {}
            if hit_meta:
                organism = hit_meta.get("organism") or None
                external_url = hit_meta.get("external_url")
                attributes = hit_meta.get("attributes") or {}

            normalized_hits.append({
                "accession": sacc,
                "name": name,
                "organism": organism,
                "source_db": src,
                "source_type": source_type,
                "external_url": external_url,
                "score": score,
                "identity": identity,
                "e_value": evalue,
                "attributes": attributes
            })

        # Optionally: deduplicate by accession + source_db, keep highest score
        dedup = {}
        for item in normalized_hits:
            key = (item["source_db"], item["accession"])
            prev = dedup.get(key)
            if prev is None or (item["score"] is not None and item["score"] > prev["score"]):
                dedup[key] = item
        result_list = list(dedup.values())

        # 3) insert into results table and update task to DONE
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO results (task_id, total, results) VALUES (%s, %s, %s)",
                            (task_id, len(result_list), json.dumps(result_list)))
                cur.execute("UPDATE tasks SET status=%s WHERE id=%s", ("DONE", task_id))
        print(f"Imported {len(result_list)} hits for task {task_id}")

    except Exception as e:
        # record failure into tasks.error
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE tasks SET status=%s, error=%s WHERE id=%s", ("FAILED", str(e), task_id))
        except Exception:
            pass
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()

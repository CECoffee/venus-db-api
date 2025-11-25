import asyncio
import random
from typing import Dict, Any, List

from queue_redis import pop_task_from_queue, set_task_fields, set_task_results, get_task
from utils.database import fetch
from utils.redis_cli import get_redis
from utils.scope_proceed import normalize_scopes

CONCURRENCY = 2
PROCESS_TIME_SECONDS = 2  # each task simulated processing time

redis = get_redis()


async def generate_mock_results(task: Dict[str, Any]) -> Dict[str, Any]:
    content = task.get("content", "")
    detected_mode = task.get("detected_mode", task.get("input_mode", "TEXT"))
    requested_scope = task.get("requested_db_scope", [])

    rows = await fetch(
        "SELECT id, source_type FROM databases WHERE id = ANY($1::text[])",
        requested_scope,
    )
    types = [r for r in rows]

    # build helper lists/dict
    id_to_type = {r["id"]: r["source_type"] for r in types}
    external_dbs = [r["id"] for r in types if (r["source_type"] or "").lower() == "external"]
    internal_dbs = [r["id"] for r in types if (r["source_type"] or "").lower() != "external"]

    # fallback: if requested_scope empty or fetch returned nothing, still create some plausible db names
    if not types and requested_scope:
        # If requested_scope contains plain names but DB fetch failed, seed them as internal
        id_to_type = {s: "internal" for s in requested_scope}
        internal_dbs = list(requested_scope)
        external_dbs = []

    # helper to choose a source db and type
    def pick_source():
        """
        Return (src_db, src_type) chosen based on availability.
        Prefer external with probability depending on how many external dbs exist.
        """
        if external_dbs and internal_dbs:
            # probability proportional to counts with a slight bias to internal
            p_ext = max(0.15, min(0.7, len(external_dbs) / (len(types) + 0.0)))
            if random.random() < p_ext:
                db = random.choice(external_dbs)
                return db, id_to_type.get(db, "external")
            else:
                db = random.choice(internal_dbs)
                return db, id_to_type.get(db, "internal")
        elif external_dbs:
            db = random.choice(external_dbs)
            return db, id_to_type.get(db, "external")
        elif internal_dbs:
            db = random.choice(internal_dbs)
            return db, id_to_type.get(db, "internal")
        else:
            # complete fallback
            db = "mock_internal_db"
            return db, "internal"

    def make_accession_for(db_name: str, src_type: str) -> str:
        dn = (db_name or "").lower()
        if "uniprot" in dn or "uniprot_kb" in dn:
            return f"P{random.randint(10000,99999)}"
        if "ncbi" in dn or "refseq" in dn:
            # NP_XXXXX or XP_
            prefix = random.choice(["NP_", "XP_"])
            return f"{prefix}{random.randint(100000,999999)}"
        if "genbank" in dn:
            return f"GB{random.randint(1000000,9999999)}"
        if "deep_sea" in dn or "meer" in dn or "ocean" in dn:
            return f"DS_{random.randint(1000,999999)}"
        if src_type == "external":
            return f"EXT{random.randint(10000,999999)}"
        # default internal accession
        return f"DS_{random.randint(1000,999999)}"

    def make_attributes_for(db_name: str, src_type: str) -> Dict[str, Any]:
        attrs: Dict[str, Any] = {}
        dn = (db_name or "").lower()
        # Internal biosample-like attributes
        if "deep_sea" in dn or "meer" in dn or "ocean" in dn:
            attrs["depth_m"] = random.randint(100, 8000)
            attrs["pressure_bar"] = round(random.uniform(10.0, 400.0), 2)
            if random.random() < 0.3:
                attrs["device"] = random.choice(["ROV", "AUV", "CTD"])
        if "salt" in dn or "saline" in dn or "salt_lake" in dn:
            attrs["ph"] = round(random.uniform(6.5, 11.5), 2)
            attrs["salinity_psu"] = round(random.uniform(5.0, 300.0), 2)
        if "soil" in dn or "sediment" in dn:
            attrs["moisture_pct"] = round(random.uniform(1.0, 60.0), 1)
            attrs["organic_matter_pct"] = round(random.uniform(0.1, 15.0), 2)
        # External protein-like attributes
        if "uniprot" in dn or "uniprot_kb" in dn or src_type == "external":
            attrs["length"] = random.randint(50, 2000)
            attrs["taxonomy"] = random.choice(["Homo sapiens", "Escherichia coli", "Saccharomyces cerevisiae", "Arabidopsis thaliana"])
            if random.random() < 0.2:
                attrs["reviewed"] = random.choice([True, False])
        # Generic metadata
        if not attrs:
            # add some lightweight attrs
            if random.random() < 0.3:
                attrs["sample_date"] = f"202{random.randint(0,4)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            if random.random() < 0.2:
                attrs["notes"] = random.choice(["simulated", "test data", "synthetic"])
        return attrs

    # decide how many results
    n = random.randint(3, 25)
    results: List[Dict[str, Any]] = []

    # influence factors
    scope_size = max(1, len(requested_scope)) if isinstance(requested_scope, (list, tuple)) else 1
    for i in range(n):
        # pick source
        src_db, src_type = pick_source()

        # accession depending on db
        acc = make_accession_for(src_db, src_type)

        # attributes derived from db name and type
        attributes = make_attributes_for(src_db, src_type)

        # scoring: base score, then nudged by identity (if sequence) and scope size
        base_score = random.uniform(40.0, 95.0)
        # if the requested scope is narrow (single db) we slightly boost score to simulate better-specific matches
        if scope_size == 1:
            base_score += random.uniform(0, 6.0)
        # small randomness decay with i so top hits slightly better
        base_score += max(0, (n - i) * random.uniform(0.0, 0.3))
        score = round(min(100.0, base_score), 2)

        identity = None
        e_value = None
        if detected_mode == "SEQUENCE":
            # identity: more likely to be higher for external curated DBs (like uniprot)
            id_base = random.uniform(15.0, 98.0)
            if "uniprot" in (src_db or "").lower() or src_type == "external":
                id_base += random.uniform(0, 6.0)
            # narrow scope -> slightly higher identity on average
            if scope_size == 1:
                id_base += random.uniform(0, 4.0)
            identity = round(min(100.0, id_base), 2)

            # e_value: sample log-uniform between 1e-200 and 1e-1, but keep as float
            log10_ev = random.uniform( -200.0, -1.0 )  # exponent range
            ev = 10.0 ** (log10_ev)
            # occasionally produce a small non-exponential e-value (simulating weird tools)
            if random.random() < 0.08:
                ev = random.expovariate(1.0)
            # avoid underflow to 0 by capping
            if ev < 1e-300:
                ev = 0.0
            # keep a readable representation but as float
            e_value = float("{:.6g}".format(ev))

        # make name and organism plausibly varied
        name = f"Mock entry {acc}"
        organism = attributes.get("taxonomy", "Mockus exemplar") if attributes else "Mockus exemplar"

        # external_url for external DBs
        external_url = None
        lower_db = (src_db or "").lower()
        if "uniprot" in lower_db:
            external_url = f"https://www.uniprot.org/uniprot/{acc}"
        elif "ncbi" in lower_db or "refseq" in lower_db:
            external_url = f"https://www.ncbi.nlm.nih.gov/protein/{acc}"
        elif src_type == "external":
            external_url = f"https://example.org/{src_db}/{acc}"

        result = {
            "accession": acc,
            "name": name,
            "organism": organism,
            "source_db": src_db,
            "source_type": src_type,
            "external_url": external_url,
            "score": score,
            "identity": identity,
            "e_value": e_value,
            "attributes": attributes
        }
        results.append(result)

    payload = {
        "total": len(results),
        "results": results
    }
    return payload

async def process_task(task_id: str):
    # fetch task metadata from redis
    task = await get_task(task_id)
    if not task:
        return
    # mark RUNNING
    await set_task_fields(task_id, {"status": "RUNNING", "progress": 5})
    # simulate processing
    await asyncio.sleep(PROCESS_TIME_SECONDS)
    # generate mock results
    payload = await generate_mock_results(task)
    # write results into redis hash and set DONE
    await set_task_results(task_id, payload["results"], payload["total"])
    # schedule transfer to Postgres after 10 minutes (600s)
    # asyncio.create_task(schedule_transfer_to_postgres_after_delay(task_id, delay_seconds=600))

async def worker_loop(stop_event: asyncio.Event):
    sem = asyncio.Semaphore(CONCURRENCY)
    while not stop_event.is_set():
        task_id = await pop_task_from_queue(block_timeout=5)
        if not task_id:
            await asyncio.sleep(0.1)
            continue
        # process in background respecting concurrency
        await sem.acquire()
        # run process_task in background to allow loop to continue
        async def _run_and_release(tid):
            try:
                await process_task(tid)
            finally:
                sem.release()
        asyncio.create_task(_run_and_release(task_id))

async def start_workers(app):
    stop_event = asyncio.Event()
    app.state.worker_stop_event = stop_event
    # launch loop as background task
    app.state.worker_task = asyncio.create_task(worker_loop(stop_event))

async def stop_workers(app):
    if hasattr(app.state, "worker_stop_event"):
        app.state.worker_stop_event.set()
    if hasattr(app.state, "worker_task"):
        await app.state.worker_task
